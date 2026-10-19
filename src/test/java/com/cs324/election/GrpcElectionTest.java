package com.cs324.election;

import com.cs324.bootstrap.BootstrapGrpcService;
import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.computation.RegistryClient;
import com.cs324.election.proto.ElectionMessage;
import com.cs324.election.proto.ElectionServiceGrpc;
import com.cs324.registry.proto.RegisterRequest;
import com.cs324.registry.proto.RegistryServiceGrpc;
import com.cs324.worker.WorkerRemoteImpl;
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
import java.net.ServerSocket;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
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

/**
 * Election over real TCP gRPC endpoints.
 *
 * <p>The topology is wired the way production wires it - through the RMI peer links that
 * still own neighbour membership - and then the RMI registry is stopped before any election
 * runs. Convergence after that point is only possible if every wave hop, echo and
 * announcement travelled over {@code ElectionService}.
 *
 * <p>Each worker's published endpoint is a second listener that hosts the same
 * {@link ElectionHandler} behind a counting interceptor, so "a duplicate is answered, not
 * dropped" is observed on the wire rather than inferred from a converged coordinator. The
 * directory is told about that port with the ordinary {@code RegistryService} update, which
 * is the same call a worker makes for its own endpoint.
 */
class GrpcElectionTest {

    private static final String HOST = "localhost";
    private static final long CONVERGENCE_TIMEOUT_MS = 15_000;

    private final Map<Integer, WorkerRemoteImpl> workers = new LinkedHashMap<>();
    private final Map<Integer, Server> endpoints = new LinkedHashMap<>();
    private final Map<Integer, AtomicInteger> echoCounts = new ConcurrentHashMap<>();
    private final List<Server> servers = new ArrayList<>();
    private final List<Remote> exported = new ArrayList<>();
    private Registry rmiRegistry;
    private int rmiPort;
    private int bootstrapPort;
    private RegistryClient discovery;
    private ManagedChannel registryChannel;

    @BeforeEach
    void setUp() throws Exception {
        workers.clear();
        endpoints.clear();
        servers.clear();
        exported.clear();
        echoCounts.clear();

        // Real bootstrap gRPC service: peers are resolved through its registry, never guessed.
        Server bootstrap = ServerBuilder.forPort(0)
                .addService(new BootstrapGrpcService(new BootstrapRegistryImpl())).build().start();
        servers.add(bootstrap);
        bootstrapPort = bootstrap.getPort();
        discovery = new RegistryClient(HOST, bootstrapPort);
        registryChannel = ManagedChannelBuilder.forAddress(HOST, bootstrapPort).usePlaintext().build();

        // One RMI registry carries the peer links, exactly as a running cluster does.
        try (ServerSocket reservation = new ServerSocket(0)) {
            rmiPort = reservation.getLocalPort();
        }
        rmiRegistry = LocateRegistry.createRegistry(rmiPort);
    }

    /** Builds a worker, then publishes an observable endpoint that counts inbound echo RPCs. */
    private WorkerRemoteImpl addWorker(int id) throws Exception {
        WorkerRemoteImpl worker = new WorkerRemoteImpl(id);
        rmiRegistry.rebind("Worker" + id, worker);
        exported.add(worker);
        workers.put(id, worker);
        worker.startProgressServer();

        AtomicInteger echoCount = new AtomicInteger();
        echoCounts.put(id, echoCount);
        Server server = countingServer(worker, echoCount);
        servers.add(server);
        endpoints.put(id, server);

        // Identity first, then the endpoint: the directory only accepts an update for a
        // worker it already knows, which is the order a real startup uses.
        RegistryServiceGrpc.RegistryServiceBlockingStub registry = RegistryServiceGrpc
                .newBlockingStub(registryChannel).withDeadlineAfter(5, TimeUnit.SECONDS);
        registry.register(RegisterRequest.newBuilder().setWorkerId(id).setHost(HOST).setPort(rmiPort).build());
        worker.enableGrpcJobs(HOST, bootstrapPort, HOST, rmiPort);
        // The observable listener takes over as the published endpoint, so the wave and every
        // echo resolve here. Discovery stays the ordinary registry lookup either way.
        discovery.publish(id, HOST, rmiPort, server.getPort());
        return worker;
    }

    /** Wraps the worker's election service, counting the inbound echo RPCs. */
    private Server countingServer(WorkerRemoteImpl worker, AtomicInteger echoCount) throws Exception {
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
                .build().start();
    }

    private void connect(int fromId, int toId) throws Exception {
        workers.get(fromId).connectAsNeighbour(toId, HOST, rmiPort);
    }

    /** 1-2-3: no cycle, so convergence cannot lean on duplicate suppression. */
    private void wireChain() throws Exception {
        connect(1, 2);
        connect(2, 1);
        connect(2, 3);
        connect(3, 2);
    }

    /** 1-2-3-1: the smallest cyclic shape, so each hop can meet the same wave twice. */
    private void wireTriangle() throws Exception {
        wireChain();
        connect(3, 1);
        connect(1, 3);
    }

    /** Closes the peer links, leaving gRPC as the only way a wave can travel. */
    private void stopRmiPeerLinks() throws Exception {
        UnicastRemoteObject.unexportObject(rmiRegistry, true);
        rmiRegistry = null;
    }

    private static void seedJobAllocationCounter(WorkerRemoteImpl worker, int value) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("jobAllocationCounter");
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

    @Test
    void chainTopologyConvergesOverGrpcWithoutRmiPeerLinks() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        wireChain();
        stopRmiPeerLinks();

        workers.get(2).startElection();

        awaitAgreement(3, 1, 2, 3);
    }

    @Test
    void triangleTopologyConvergesOnOneCoordinatorOverGrpc() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        wireTriangle();
        stopRmiPeerLinks();

        workers.get(1).startElection();

        // All three sit on JAC 0, so the id tie-break decides: worker 3 wins.
        awaitAgreement(3, 1, 2, 3);
    }

    @Test
    void lowestJacWinsAndTheHighestIdBreaksTies() throws Exception {
        WorkerRemoteImpl first = addWorker(1);
        WorkerRemoteImpl second = addWorker(2);
        WorkerRemoteImpl third = addWorker(3);
        wireTriangle();
        stopRmiPeerLinks();
        seedJobAllocationCounter(first, 4);
        seedJobAllocationCounter(second, 1);
        seedJobAllocationCounter(third, 1);

        first.startElection();

        // Two and three share the lowest JAC, so only the id rule can separate them.
        awaitAgreement(3, 1, 2, 3);
    }

    @Test
    void duplicateWaveIsAnsweredWithAnEchoAndNotForwarded() throws Exception {
        WorkerRemoteImpl first = addWorker(1);
        WorkerRemoteImpl second = addWorker(2);
        addWorker(3);
        wireTriangle();
        stopRmiPeerLinks();

        long waveId = first.startElection();
        awaitAgreement(3, 1, 2, 3);

        // Replay exactly the wave worker 1 already sent, claiming worker 2 as the parent:
        // the duplicate path must answer with an echo and stop there.
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
        WorkerRemoteImpl first = addWorker(1);
        addWorker(2);
        addWorker(3);
        wireChain();
        stopRmiPeerLinks();

        // Worker 3 disappears before the wave reaches it. Its endpoint stays published, so the
        // wave has to fail on the hop rather than wait for an echo that can never arrive.
        Server third = endpoints.get(3);
        third.shutdownNow();
        assertTrue(third.awaitTermination(5, TimeUnit.SECONDS));

        first.startElection();

        // 1 and 2 remain, and the tie goes to the higher id, so worker 2 must win.
        awaitAgreement(2, 1, 2);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (WorkerRemoteImpl worker : workers.values()) {
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
        if (rmiRegistry != null) {
            for (String name : rmiRegistry.list()) {
                rmiRegistry.unbind(name);
            }
            UnicastRemoteObject.unexportObject(rmiRegistry, true);
        }
        for (Remote remote : exported) {
            try {
                UnicastRemoteObject.unexportObject(remote, true);
            } catch (NoSuchObjectException ignored) {
                // Already gone, so a second attempt is not a failure.
            }
        }
    }
}