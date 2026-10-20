package com.cs324.worker;

import com.cs324.bootstrap.*;
import com.cs324.computation.*;
import com.cs324.common.WorkerEvent;
import com.cs324.election.proto.*;
import com.cs324.job.proto.*;
import io.grpc.*;
import org.junit.jupiter.api.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** Exercises actual discovery, control, and job RPCs. */
class GrpcControlRegressionTest {
    private final List<WorkerRuntime> workers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final Map<Integer, CompletableFuture<Object>> results = new ConcurrentHashMap<>();
    private final AtomicInteger callbacks = new AtomicInteger();
    private Server bootstrap, client;
    private RegistryClient directory;

    @BeforeEach void setup() throws Exception {
        bootstrap = ServerBuilder.forPort(0).addService(new BootstrapGrpcService(new BootstrapRegistryImpl())).build().start();
        directory = new RegistryClient("localhost", bootstrap.getPort());
        client = ServerBuilder.forPort(0).addService(new ClientResultService((id, result) -> {
            callbacks.incrementAndGet();
            results.computeIfAbsent(id, key -> new CompletableFuture<>()).complete(result);
        })).build().start();
    }

    private WorkerRuntime worker(int id) throws Exception {
        return worker(new WorkerRuntime(id));
    }

    private WorkerRuntime worker(WorkerRuntime worker) throws Exception {
        int id = worker.getId();
        directory.registerWorker(id, "localhost", 0);
        workers.add(worker);
        worker.startProgressServer();
        worker.setBootstrapReference(directory, "localhost");
        worker.enableGrpcJobs("localhost", bootstrap.getPort(), "localhost", 0);
        return worker;
    }

    private ManagedChannel channel(WorkerRuntime worker) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", worker.getProgressPort()).usePlaintext().build();
        channels.add(channel);
        return channel;
    }

    private void link(WorkerRuntime a, WorkerRuntime b) {
        new GrpcWorkerClient(directory.worker(a.getId())).connectAsNeighbour(b.getId(), "", 0);
        new GrpcWorkerClient(directory.worker(b.getId())).connectAsNeighbour(a.getId(), "", 0);
    }

    private void await(java.util.function.BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(25);
        assertTrue(condition.getAsBoolean());
    }

    private SubmitJobRequest request(int id) {
        return SubmitJobRequest.newBuilder().setJobId(id).setJobType("MAX")
                .setPayload(ComputationCodec.encode(new int[]{3, 9, 4, 7}))
                .setClient(ClientEndpoint.newBuilder().setHost("localhost").setPort(client.getPort())).build();
    }

    private JobServiceGrpc.JobServiceBlockingStub jobs(WorkerRuntime worker) {
        return JobServiceGrpc.newBlockingStub(channel(worker)).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    private void leader(WorkerRuntime a, WorkerRuntime b) throws Exception {
        link(a, b);
        a.startElection();
        await(() -> a.getCurrentCoordinatorId() == b.getId() && b.getCurrentCoordinatorId() == b.getId());
        awaitReady(b);
    }

    private void awaitReady(WorkerRuntime worker) throws Exception {
        // The control view is published before asynchronous checkpoint merging finishes.
        var gate = WorkerRuntime.class.getDeclaredField("failoverInProgress");
        gate.setAccessible(true);
        await(() -> {
            try { return !((java.util.concurrent.atomic.AtomicBoolean) gate.get(worker)).get(); }
            catch (IllegalAccessException e) { throw new AssertionError(e); }
        });
    }

    @Test void peerlessSubmissionFailsWithoutDispatchOrSpendingTermAndCanRetry() throws Exception {
        var one = worker(1);
        one.adoptGrpcCoordinator(1, 1);
        awaitReady(one);
        var jobs = jobs(one);
        var request = request(101);
        for (int i = 0; i < 7; i++) {
            var failure = assertThrows(StatusRuntimeException.class, () -> jobs.submitJob(request));
            assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
        }
        assertEquals(0, one.getJobsCoordinatedInTerm());
        assertEquals(0, one.getJobAllocationCounter());
        assertEquals(0, callbacks.get());
        assertTrue(one.getRecentEvents(-1).stream().noneMatch(e -> e.kind().equals(WorkerEvent.TASK_DISPATCHED)));
        worker(2);
        jobs.submitJob(request);
        assertEquals(9, results.computeIfAbsent(101, key -> new CompletableFuture<>()).get(10, TimeUnit.SECONDS));
        assertEquals(1, one.getJobsCoordinatedInTerm());
    }

    @Test void duplicateSubmissionUsesOneTermSlotAndOneCallback() throws Exception {
        var one = worker(1); var two = worker(2); leader(one, two);
        var jobs = jobs(two); var request = request(102);
        jobs.submitJob(request); jobs.submitJob(request);
        assertEquals(9, results.computeIfAbsent(102, key -> new CompletableFuture<>()).get(10, TimeUnit.SECONDS));
        jobs.submitJob(request);
        assertEquals(1, two.getJobsCoordinatedInTerm());
        assertEquals(1, callbacks.get());
        assertEquals(1, two.getJobAllocationCounter());
    }

    @Test void fifthAcceptedJobRotatesAndDuplicateAnnouncementCannotResetBudget() throws Exception {
        var one = worker(1); var two = worker(2); leader(one, two);
        var jobs = jobs(two);
        for (int i = 1; i <= 4; i++) {
            jobs.submitJob(request(i));
            assertEquals(9, results.computeIfAbsent(i, key -> new CompletableFuture<>()).get(10, TimeUnit.SECONDS));
        }
        long term = two.heartbeat(0, 0, 0).getTerm();
        ElectionServiceGrpc.newBlockingStub(channel(two)).announceCoordinator(CoordinatorAnnouncement.newBuilder()
                .setCoordinatorId(2).setTerm(term).setMessageId(99999).build());
        assertEquals(4, two.getJobsCoordinatedInTerm());
        jobs.submitJob(request(5));
        assertEquals(9, results.computeIfAbsent(5, key -> new CompletableFuture<>()).get(10, TimeUnit.SECONDS));
        await(() -> one.getCurrentCoordinatorId() == 1 && two.getCurrentCoordinatorId() == 1);
        assertEquals(Status.Code.FAILED_PRECONDITION,
                assertThrows(StatusRuntimeException.class, () -> jobs.submitJob(request(6))).getStatus().getCode());
    }

    @Test void gracefulLeaveRepairsBothEdgesOverGrpc() throws Exception {
        var one = worker(1); var two = worker(2); var three = worker(3);
        link(one, two); link(two, three);
        two.notifyNeighboursOfLeaving();
        await(() -> one.getNeighbourIds().contains(3) && three.getNeighbourIds().contains(1));
        assertFalse(one.getNeighbourIds().contains(2));
        assertFalse(three.getNeighbourIds().contains(2));
        assertTrue(one.getRecentEvents(-1).stream().anyMatch(e -> e.kind().equals(WorkerEvent.NEIGHBOUR_ADDED)));
    }

    @Test void concurrentSubmissionsCannotOverspendTheFiveJobTerm() throws Exception {
        var releaseElection = new CountDownLatch(1);
        var one = worker(1);
        var two = worker(new WorkerRuntime(2) {
            @Override public long startElection() {
                // Keep the original term closed while all concurrent RPCs finish.
                // Otherwise a legitimate new term could accept some of the callers.
                try {
                    if (!releaseElection.await(20, TimeUnit.SECONDS))
                        throw new AssertionError("test did not release the election");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return super.startElection();
            }
        });
        leader(one, two);
        long initialTerm = two.heartbeat(0, 0, 0).getTerm();
        var jobs = jobs(two);
        var callers = Executors.newFixedThreadPool(12);
        try {
            List<Callable<Boolean>> calls = new ArrayList<>();
            for (int id = 200; id < 212; id++) {
                var request = request(id);
                calls.add(() -> {
                    try { jobs.submitJob(request); return true; }
                    catch (StatusRuntimeException error) {
                        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
                        return false;
                    }
                });
            }
            int accepted = 0;
            for (var result : callers.invokeAll(calls, 20, TimeUnit.SECONDS))
                if (result.get()) accepted++;
            assertEquals(5, accepted);
            releaseElection.countDown();
            await(() -> one.getCurrentCoordinatorId() == two.getCurrentCoordinatorId()
                    && two.heartbeat(0, 0, 0).getTerm() > initialTerm);
        } finally {
            releaseElection.countDown();
            callers.shutdownNow();
        }
    }

    @Test void crashedBridgeIsRemovedAndSurvivorsReconnect() throws Exception {
        var one = worker(1); var two = worker(2); var three = worker(3);
        link(one, two); link(two, three);
        two.shutdown();
        one.startElection();
        await(() -> one.getNeighbourIds().contains(3));
        assertFalse(one.getNeighbourIds().contains(2));
        one.startElection();
        await(() -> one.getCurrentCoordinatorId() == 3 && three.getCurrentCoordinatorId() == 3);
    }

    @Test void staleOrTermlessAnnouncementCannotReplaceLeader() throws Exception {
        var one = worker(1); var two = worker(2); leader(one, two);
        var election = ElectionServiceGrpc.newBlockingStub(channel(one));
        election.announceCoordinator(CoordinatorAnnouncement.newBuilder().setCoordinatorId(1).setTerm(0).setMessageId(700).build());
        assertEquals(2, one.getCurrentCoordinatorId());
        assertEquals(0, one.getJobsCoordinatedInTerm());
    }

    @AfterEach void cleanup() throws Exception {
        for (var worker : workers) worker.shutdown();
        for (var channel : channels) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (directory != null) directory.close();
        if (client != null) client.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (bootstrap != null) bootstrap.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
}
