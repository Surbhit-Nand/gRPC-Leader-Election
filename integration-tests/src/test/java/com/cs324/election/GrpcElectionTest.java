package com.cs324.election;

import com.cs324.bootstrap.BootstrapGrpcService;
import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.computation.RegistryClient;
import com.cs324.election.proto.ElectionMessage;
import com.cs324.election.proto.ElectionServiceGrpc;
import com.cs324.registry.proto.RegisterRequest;
import com.cs324.registry.proto.RegistryServiceGrpc;
import com.cs324.worker.WorkerRuntime;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// Exercise election waves, echoes and announcements over real gRPC endpoints with observable echo counts.
class GrpcElectionTest {

    private static final String HOST = "localhost";
    private static final long CONVERGENCE_TIMEOUT_MS = 15_000;

    private final Map<Integer, WorkerRuntime> workers = new LinkedHashMap<>();
    private final Map<Integer, Server> endpoints = new LinkedHashMap<>();
    private final Map<Integer, AtomicInteger> echoCounts = new ConcurrentHashMap<>();
    private final List<Server> servers = new ArrayList<>();
    private int bootstrapPort;
    private RegistryClient discovery;
    private ManagedChannel registryChannel;

    @BeforeEach
    void setUp() throws Exception {
        workers.clear();
        endpoints.clear();
        servers.clear();
        echoCounts.clear();

        // Real bootstrap gRPC service: peers are resolved through its registry, never guessed.
        Server bootstrap = ServerBuilder.forPort(0)
                .addService(new BootstrapGrpcService(new BootstrapRegistryImpl())).build().start();
        servers.add(bootstrap);
        bootstrapPort = bootstrap.getPort();
        discovery = new RegistryClient(HOST, bootstrapPort);
        registryChannel = ManagedChannelBuilder.forAddress(HOST, bootstrapPort).usePlaintext().build();
    }

    // Builds a worker, then publishes an observable endpoint that counts inbound echo RPCs.
    private WorkerRuntime addWorker(int id) throws Exception {
        WorkerRuntime worker = new WorkerRuntime(id);
        workers.put(id, worker);
        worker.startProgressServer();

        AtomicInteger echoCount = new AtomicInteger();
        echoCounts.put(id, echoCount);
        Server server = countingServer(worker, echoCount);
        servers.add(server);
        endpoints.put(id, server);

        // Register the worker identity before publishing its listener endpoint.
        RegistryServiceGrpc.RegistryServiceBlockingStub registry = RegistryServiceGrpc
                .newBlockingStub(registryChannel).withDeadlineAfter(5, TimeUnit.SECONDS);
        registry.register(RegisterRequest.newBuilder().setWorkerId(id).setHost(HOST).setPort(0).build());
        worker.enableGrpcJobs(HOST, bootstrapPort, HOST, 0);
        // Publish the counting listener so all election messages pass through it.
        discovery.publish(id, HOST, 0, server.getPort());
        return worker;
    }

    // Wraps the worker's election service, counting the inbound echo RPCs.
    private Server countingServer(WorkerRuntime worker, AtomicInteger echoCount) throws Exception {
        ServerInterceptor counter = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                    Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                if ("SendEcho".equals(call.getMethodDescriptor().getBareMethodName())) {
                    echoCount.incrementAndGet();
                }
                return next.startCall(call, headers);
            }
        };
        return ServerBuilder.forPort(0)
                .addService(io.grpc.ServerInterceptors.intercept(new ElectionGrpcService(worker), counter))
                .addService(new com.cs324.computation.WorkerControlService(worker))
                .build().start();
    }

    private void connect(int fromId, int toId) throws Exception {
        workers.get(fromId).connectAsNeighbour(toId, HOST, 0);
    }

    // 1-2-3: no cycle, so convergence cannot lean on duplicate suppression.
    private void wireChain() throws Exception {
        connect(1, 2);
        connect(2, 1);
        connect(2, 3);
        connect(3, 2);
    }

    // 1-2-3-1: the smallest cyclic shape, so each hop can meet the same wave twice.
    private void wireTriangle() throws Exception {
        wireChain();
        connect(3, 1);
        connect(1, 3);
    }

    // Closes the peer links, leaving gRPC as the only way a wave can travel.


    private static void seedJobAllocationCounter(WorkerRuntime worker, int value) throws Exception {
        Field field = WorkerRuntime.class.getDeclaredField("jobAllocationCounter");
        field.setAccessible(true);
        ((AtomicInteger) field.get(worker)).set(value);
    }

    private void awaitAgreement(int expectedCoordinatorId, int... participants) throws Exception {
        long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            boolean agreed = true;
            for (int id : participants) {
                if (workers.get(id).getCurrentCoordinatorId() != expectedCoordinatorId) {
                    agreed = false;
                    break;
                }
            }
            if (agreed) {
                return;
            }
            Thread.sleep(20);
        }
        StringBuilder state = new StringBuilder();
        for (int id : participants) {
            state.append(" w").append(id).append('=').append(workers.get(id).getCurrentCoordinatorId());
        }
        fail("no agreement on coordinator " + expectedCoordinatorId + " within " + CONVERGENCE_TIMEOUT_MS
                + "ms over gRPC. Got:" + state);
    }

    // The recent happenings of one worker, for failure messages.
    private String feed(int workerId) {
        StringBuilder feed = new StringBuilder();
        for (com.cs324.common.WorkerEvent event : workers.get(workerId).getRecentEvents(0)) {
            feed.append(event.kind()).append(' ').append(event.detail()).append('\n');
        }
        return feed.toString();
    }

    private void awaitNoCoordinator(int... participants) throws Exception {
        long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            boolean none = true;
            for (int id : participants) {
                if (workers.get(id).getCurrentCoordinatorId() != -1) {
                    none = false;
                    break;
                }
            }
            if (none) {
                return;
            }
            Thread.sleep(20);
        }
        StringBuilder state = new StringBuilder();
        for (int id : participants) {
            state.append(" w").append(id).append('=').append(workers.get(id).getCurrentCoordinatorId());
        }
        fail("a minority group still has a coordinator after " + CONVERGENCE_TIMEOUT_MS
                + "ms. Got:" + state);
    }

    @Test
    void chainTopologyConvergesOverGrpcControlLinks() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        wireChain();


        workers.get(2).startElection();

        awaitAgreement(3, 1, 2, 3);
    }

    @Test
    void triangleTopologyConvergesOnOneCoordinatorOverGrpc() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        wireTriangle();


        workers.get(1).startElection();

        // All three sit on JAC 0, so the id tie-break decides: worker 3 wins.
        awaitAgreement(3, 1, 2, 3);
    }

    @Test
    void healedPartitionsCannotElectMinorityLeadersAndThenConverge() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        addWorker(4);
        // Two isolated groups of two. A majority of four is three, so neither side may lead.
        connect(1, 2);
        connect(2, 1);
        connect(3, 4);
        connect(4, 3);

        workers.get(1).startElection();
        workers.get(3).startElection();
        awaitNoCoordinator(1, 2, 3, 4);

        // After reconnecting, elect the higher-ID tied candidate in a term newer than both old waves.
        connect(2, 3);
        connect(3, 2);
        workers.get(1).startElection();

        awaitAgreement(4, 1, 2, 3, 4);
    }

    @Test
    void minorityLeaderStepsDownWhenItCanNoLongerReachAMajority() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        addWorker(4);
        connect(1, 2);
        connect(2, 1);
        connect(2, 3);
        connect(3, 2);
        connect(3, 4);
        connect(4, 3);

        workers.get(1).startElection();
        awaitAgreement(4, 1, 2, 3, 4);

        // Workers 4 and 3 remain alive, but a majority of four needs three votes.
        for (int id : new int[]{1, 2}) {
            Server endpoint = endpoints.get(id);
            endpoint.shutdownNow();
            assertTrue(endpoint.awaitTermination(5, TimeUnit.SECONDS));
        }

        awaitNoCoordinator(4);
        assertTrue(workers.get(4).getRecentEvents(0).stream()
                        .anyMatch(event -> event.kind().equals(com.cs324.common.WorkerEvent.QUORUM_LOST)),
                () -> "the minority leader must record that it lost quorum; feed:\n" + feed(4));
    }

    @Test
    void lowestJacWinsAndTheHighestIdBreaksTies() throws Exception {
        WorkerRuntime first = addWorker(1);
        WorkerRuntime second = addWorker(2);
        WorkerRuntime third = addWorker(3);
        wireTriangle();

        seedJobAllocationCounter(first, 4);
        seedJobAllocationCounter(second, 1);
        seedJobAllocationCounter(third, 1);

        first.startElection();

        // Two and three share the lowest JAC, so only the id rule can separate them.
        awaitAgreement(3, 1, 2, 3);
    }

    @Test
    void duplicateWaveIsAnsweredWithAnEchoAndNotForwarded() throws Exception {
        WorkerRuntime first = addWorker(1);
        WorkerRuntime second = addWorker(2);
        addWorker(3);
        wireTriangle();


        long waveId = first.startElection();
        awaitAgreement(3, 1, 2, 3);

        // Replay an existing wave and verify that the duplicate gets an echo without being forwarded.
        int echoesBefore = echoCounts.get(2).get();
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(HOST, endpoints.get(1).getPort()).usePlaintext().build();
        try {
            ElectionServiceGrpc.ElectionServiceBlockingStub stub = ElectionServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);
            stub.sendElection(ElectionMessage.newBuilder()
                    .setOriginId(2).setCandidateId(1).setCandidateJac(first.getJobAllocationCounter())
                    .setMessageId(waveId).build());
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }

        assertTrue(echoCounts.get(2).get() > echoesBefore,
                "a duplicate wave must be answered with an echo, or the sender waits forever");
        assertEquals(3, first.getCurrentCoordinatorId(),
                "answering a duplicate must not change the coordinator");
        assertEquals(3, second.getCurrentCoordinatorId());
    }

    @Test
    void aDeadNeighbourDoesNotStallTheWave() throws Exception {
        WorkerRuntime first = addWorker(1);
        addWorker(2);
        addWorker(3);
        wireChain();


        // Leave the crashed worker registered so the failed RPC tests election edge repair.
        Server third = endpoints.get(3);
        third.shutdownNow();
        assertTrue(third.awaitTermination(5, TimeUnit.SECONDS));

        first.startElection();

        // 1 and 2 remain, and the tie goes to the higher id, so worker 2 must win.
        awaitAgreement(2, 1, 2);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (WorkerRuntime worker : workers.values()) {
            try {
                worker.shutdown();
            } catch (RuntimeException e) {
                System.err.println("worker shutdown failed: " + e.getMessage());
            }
        }
        if (registryChannel != null) {
            registryChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (discovery != null) {
            discovery.close();
        }
        for (Server server : servers) {
            server.shutdownNow();
        }
        for (Server server : servers) {
            server.awaitTermination(5, TimeUnit.SECONDS);
        }


    }
}
