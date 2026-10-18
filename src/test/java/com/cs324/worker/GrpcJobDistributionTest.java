package com.cs324.worker;

import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.bootstrap.BootstrapGrpcService;
import com.cs324.computation.*;
import com.cs324.job.proto.*;
import com.cs324.registry.proto.*;
import com.cs324.progress.ProgressService;
import com.cs324.progress.proto.*;
import io.grpc.*;
import java.rmi.server.UnicastRemoteObject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

/** Registry discovery, shared job callbacks and progress use real TCP gRPC calls. */
class GrpcJobDistributionTest {
    private final ExecutorService pool = Executors.newFixedThreadPool(8);
    private final List<Server> servers = new ArrayList<>();
    private final List<GrpcJobService> services = new ArrayList<>();
    private final List<java.rmi.registry.Registry> contacts = new ArrayList<>();
    private final List<WorkerRemoteImpl> controlWorkers = new ArrayList<>();
    private final Map<Integer, CompletableFuture<Object>> results = new ConcurrentHashMap<>();
    private final AtomicInteger assignments = new AtomicInteger();
    private final AtomicBoolean leader = new AtomicBoolean(true);
    private BootstrapRegistryImpl directory;
    private RegistryClient discovery;
    private ManagedChannel registryChannel, coordinatorChannel;
    private int clientPort;
    private RegistryServiceGrpc.RegistryServiceBlockingStub registry;
    private com.cs324.job.proto.JobServiceGrpc.JobServiceBlockingStub client;

    @BeforeEach void setup() throws Exception {
        directory = new BootstrapRegistryImpl();
        Server bootstrap = ServerBuilder.forPort(0).addService(new BootstrapGrpcService(directory)).build().start();
        servers.add(bootstrap);
        discovery = new RegistryClient("localhost", bootstrap.getPort());
        registryChannel = ManagedChannelBuilder.forAddress("localhost", bootstrap.getPort()).usePlaintext().build();
        registry = RegistryServiceGrpc.newBlockingStub(registryChannel).withDeadlineAfter(5, TimeUnit.SECONDS);
        Server callbacks = ServerBuilder.forPort(0).addService(new ClientResultService((id, value) ->
                results.computeIfAbsent(id, ignored -> new CompletableFuture<>()).complete(value))).build().start();
        servers.add(callbacks); clientPort = callbacks.getPort();
        int port = addWorker(1); addWorker(2); addWorker(3);
        coordinatorChannel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        client = com.cs324.job.proto.JobServiceGrpc.newBlockingStub(coordinatorChannel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }
    private int addWorker(int id) throws Exception {
        // Member 1 still probes the RMI contact during peer selection.
        int contactPort;
        try (java.net.ServerSocket reservation = new java.net.ServerSocket(0)) { contactPort = reservation.getLocalPort(); }
        java.rmi.registry.Registry contact = java.rmi.registry.LocateRegistry.createRegistry(contactPort);
        WorkerRemoteImpl controlWorker = new WorkerRemoteImpl(id);
        contact.rebind("Worker" + id, controlWorker);
        contacts.add(contact); controlWorkers.add(controlWorker);
        AtomicInteger port = new AtomicInteger();
        ProgressService progress = new ProgressService();
        GrpcJobService service = new GrpcJobService(id,
                () -> CoordinatorEndpoint.newBuilder().setWorkerId(id).setHost("localhost").setPort(port.get()).build(),
                discovery::workers, jobId -> id == 1 && leader.get(), peer -> assignments.incrementAndGet(),
                () -> {}, pool, progress, Duration.ofSeconds(5));
        Server server = ServerBuilder.forPort(0).addService(service).addService(progress).build().start();
        services.add(service); servers.add(server); port.set(server.getPort());
        registry.register(RegisterRequest.newBuilder().setWorkerId(id).setHost("localhost").setPort(contactPort).build());
        discovery.publish(id, "localhost", contactPort, server.getPort());
        return server.getPort();
    }
    @AfterEach void cleanup() throws Exception {
        if (coordinatorChannel != null) coordinatorChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (registryChannel != null) registryChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (discovery != null) discovery.close();
        services.forEach(GrpcJobService::close);
        for (Server server : servers) server.shutdownNow();
        for (Server server : servers) assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        for (WorkerRemoteImpl worker : controlWorkers) { worker.shutdown(); UnicastRemoteObject.unexportObject(worker, true); }
        for (java.rmi.registry.Registry contact : contacts) UnicastRemoteObject.unexportObject(contact, true);
        if (directory != null) UnicastRemoteObject.unexportObject(directory, true);
        pool.shutdownNow(); assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }
    private SubmitJobRequest request(String type, Object payload) {
        return SubmitJobRequest.newBuilder().setJobId(discovery.allocateJobId()).setJobType(type)
                .setPayload(ComputationCodec.encode(payload)).setClient(ClientEndpoint.newBuilder()
                        .setHost("localhost").setPort(clientPort).setName("Client")).build();
    }
    private Object run(SubmitJobRequest request) throws Exception {
        CompletableFuture<Object> result = results.computeIfAbsent(request.getJobId(), ignored -> new CompletableFuture<>());
        client.submitJob(request); return result.get(10, TimeUnit.SECONDS);
    }
    private JobProgress progress(int id) {
        var replay = JobProgressServiceGrpc.newBlockingStub(coordinatorChannel).withDeadlineAfter(5, TimeUnit.SECONDS)
                .watchJob(WatchJobRequest.newBuilder().setJobId(id).build());
        JobProgress last = null;
        while (replay.hasNext()) last = replay.next();
        return last;
    }
    @Test void maxUsesPublishedGrpcPortsAndStreamsActualWork() throws Exception {
        SubmitJobRequest request = request("MAX", new int[]{-9,-2,-5,-7,-4,-8});
        assertEquals(-2, run(request)); assertEquals(2, assignments.get());
        JobProgress last = progress(request.getJobId());
        assertTrue(last.getFinished()); assertEquals(3, last.getWorkersCount());
        assertEquals(6, last.getWorkersList().stream().mapToLong(WorkerProgress::getCompletedUnits).sum());
        assertTrue(discovery.workers().stream().allMatch(w -> w.getGrpcPort() > 0 && w.getGrpcPort() != w.getPort()));
    }
    @Test void primeSumRetains64BitResult() throws Exception {
        assertEquals(37_550_402_023L, run(request("PRIMESUM", new WorkerTaskCalculator.PrimeRange(1,1_000_000))));
    }
    @Test void primeCountKeepsDuplicatesAndSupportsEmptyInput() throws Exception {
        assertEquals(4, run(request("PRIMECOUNT", new int[]{-1,0,1,2,2,3,4,5})));
        SubmitJobRequest empty = request("PRIMECOUNT", new int[0]);
        assertEquals(0, run(empty)); assertEquals(1, progress(empty.getJobId()).getWorkersCount());
    }
    @Test void joiningWorkersEnterTheNextJobAndSmallJobsStayLocal() throws Exception {
        assertEquals(7, run(request("MAX", new int[]{7}))); assertEquals(0, assignments.get());
        addWorker(4);
        SubmitJobRequest next = request("MAX", new int[]{1,2,3,4});
        assertEquals(4, run(next)); assertEquals(4, progress(next.getJobId()).getWorkersCount());
    }
    @Test void rejectsInvalidInputAndNonCoordinator() {
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () -> run(request("MAX", new int[0])));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
        leader.set(false);
        error = assertThrows(StatusRuntimeException.class, () -> run(request("MAX", new int[]{1})));
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }
    @Test void concurrentJobsAndDuplicateSubmissionsStaySeparate() throws Exception {
        SubmitJobRequest first = request("MAX", new int[]{9,100,-3});
        SubmitJobRequest second = request("PRIMECOUNT", new int[]{2,3,4});
        try (ExecutorService clients = Executors.newFixedThreadPool(2)) {
            Future<Object> a = clients.submit(() -> run(first)); Future<Object> b = clients.submit(() -> run(second));
            assertEquals(100, a.get(10, TimeUnit.SECONDS)); assertEquals(2, b.get(10, TimeUnit.SECONDS));
        }
        int count = assignments.get(); client.submitJob(first); assertEquals(count, assignments.get());
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> client.submitJob(first.toBuilder().setPayload(ComputationCodec.encode(new int[]{5})).build()));
        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    }
    @Test void stoppedWorkerIsReassigned() throws Exception {
        servers.get(3).shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        Object result = run(request("MAX", new int[]{1,2,3}));
        assertEquals(3, result);
    }
    @Test void endpointPublicationRequiresRegisteredIdentity() {
        var address = discovery.worker(1);
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> discovery.publish(1, "wrong-host", address.getPort(), address.getGrpcPort()));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
        assertEquals(address, discovery.worker(1));
    }

    @Test void recoveredJobWithEverySliceCompletedDeliversWithoutRecomputing() throws Exception {
        SubmitJobRequest request = request("PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, 10));
        var slices = List.of(
                new com.cs324.failover.RestoredJob.RestoredSlice(1,
                        ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 5)),
                        discovery.worker(1), 1, TaskResult.newBuilder().setLongResult(10L).build()),
                new com.cs324.failover.RestoredJob.RestoredSlice(2,
                        ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(6, 10)),
                        discovery.worker(2), 1, TaskResult.newBuilder().setLongResult(7L).build()));
        CompletableFuture<Object> result = results.computeIfAbsent(request.getJobId(), ignored -> new CompletableFuture<>());
        assertTrue(services.get(0).restore(new com.cs324.failover.RestoredJob(request, slices, false), 500));
        assertEquals(17L, result.get(5, TimeUnit.SECONDS));
        assertEquals(0, assignments.get(), "completed slices must not execute again");
        assertTrue(progress(request.getJobId()).getFinished());
        assertFalse(services.get(0).coordinatedJobIds().contains(request.getJobId()));
    }
}
