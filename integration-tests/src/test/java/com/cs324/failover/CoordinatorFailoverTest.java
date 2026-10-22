package com.cs324.failover;

import com.cs324.bootstrap.BootstrapGrpcService;
import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.computation.ClientResultService;
import com.cs324.computation.ComputationCodec;
import com.cs324.common.WorkerEvent;
import com.cs324.job.proto.ClientEndpoint;
import com.cs324.job.proto.JobServiceGrpc;
import com.cs324.job.proto.SubmitJobRequest;
import com.cs324.job.proto.TaskResult;
import com.cs324.progress.proto.JobProgress;
import com.cs324.progress.proto.JobProgressServiceGrpc;
import com.cs324.progress.proto.WatchJobRequest;
import com.cs324.progress.proto.WorkerProgress;
import com.cs324.registry.proto.RegisterRequest;
import com.cs324.registry.proto.RegistryServiceGrpc;
import com.cs324.worker.WorkerRuntime;
import com.cs324.worker.WorkerTaskCalculator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Crash a coordinator during an in-flight job and verify election, recovery, result and progress over gRPC.
class CoordinatorFailoverTest {

    // Assert the job is still running before each crash instead of assuming a fixed computation time.
    private static final int RANGE_END = 10_000_000;
    // Expected prime sum computed independently with WorkerTaskCalculator for RANGE_END.
    private static final long ORACLE_SUM = 3_203_324_994_356L;
    // Small, independently known answer used to pin the arithmetic in this same cluster.
    private static final long KNOWN_SMALL_SUM = 76_127L;
    private static final long FAILOVER_BOUND_MS = 90_000;

    private final Map<Integer, WorkerRuntime> workers = new LinkedHashMap<>();
    private final List<Server> servers = new ArrayList<>();
private final List<ManagedChannel> channels = new ArrayList<>();
    // Counted per job: "delivered exactly once" is a claim about one job, not about the run.
    private final Map<Integer, AtomicInteger> deliveriesByJob = new ConcurrentHashMap<>();
    // Answers the client acts on, and anything a dying coordinator still managed to say.
    private final Map<Integer, CompletableFuture<Object>> answers = new ConcurrentHashMap<>();
    private final Map<Integer, List<Object>> rejectedAnswers = new ConcurrentHashMap<>();
    private final AtomicInteger nextJobId = new AtomicInteger(1);
    private int contactPort;
    private int bootstrapPort;
    private int clientPort;

    @BeforeEach
    void setUp() throws Exception {
        workers.clear();
        servers.clear();
channels.clear();
        answers.clear();
        rejectedAnswers.clear();
        deliveriesByJob.clear();
        nextJobId.set(1);

        Server bootstrap = ServerBuilder.forPort(0)
                .addService(new BootstrapGrpcService(new BootstrapRegistryImpl())).build().start();
        servers.add(bootstrap);
        bootstrapPort = bootstrap.getPort();

Server callbacks = ServerBuilder.forPort(0).addService(new ClientResultService((jobId, value) -> {
            deliveriesByJob.computeIfAbsent(jobId, key -> new AtomicInteger()).incrementAndGet();
            if (value instanceof Number) {
                // Keep the first client result per job, matching the GUI callback behavior.
                answers.computeIfAbsent(jobId, key -> new CompletableFuture<>()).complete(value);
            } else {
                rejectedAnswers.computeIfAbsent(jobId, key -> new CopyOnWriteArrayList<>()).add(value);
            }
        })).build().start();
        servers.add(callbacks);
        clientPort = callbacks.getPort();

        try (ServerSocket reservation = new ServerSocket(0)) {
            contactPort = reservation.getLocalPort();
        }
    }

    // One worker with its own gRPC listener, published through the directory as in production.
    private WorkerRuntime addWorker(int id) throws Exception {
        WorkerRuntime worker = new WorkerRuntime(id);
        workers.put(id, worker);
        worker.startProgressServer();
        registry().register(RegisterRequest.newBuilder()
                .setWorkerId(id).setHost("localhost").setPort(contactPort).build());
        worker.enableGrpcJobs("localhost", bootstrapPort, "localhost", contactPort);
        return worker;
    }

    private RegistryServiceGrpc.RegistryServiceBlockingStub registry() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", bootstrapPort)
                .usePlaintext().build();
        channels.add(channel);
        return RegistryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    // A ring: everyone is two hops from everyone, so no topology decides the outcome.
    private void wireRing() throws Exception {
        int[] ids = workers.keySet().stream().mapToInt(Integer::intValue).toArray();
        for (int i = 0; i < ids.length; i++) {
            workers.get(ids[i]).connectAsNeighbour(ids[(i + 1) % ids.length], "localhost", contactPort);
            workers.get(ids[(i + 1) % ids.length]).connectAsNeighbour(ids[i], "localhost", contactPort);
        }
    }

    private ManagedChannel channelTo(WorkerRuntime worker) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", worker.getProgressPort()).usePlaintext().build();
        channels.add(channel);
        return channel;
    }

    private int submitJob(int coordinatorId, String jobType, Object payload) {
        int jobId = nextJobId.getAndIncrement();
        answers.put(jobId, new CompletableFuture<>());
        SubmitJobRequest request = SubmitJobRequest.newBuilder()
                .setJobId(jobId)
                .setJobType(jobType)
                .setPayload(ComputationCodec.encode(payload))
                .setClient(ClientEndpoint.newBuilder().setHost("localhost").setPort(clientPort))
                .build();
        JobServiceGrpc.JobServiceBlockingStub stub = JobServiceGrpc
                .newBlockingStub(channelTo(workers.get(coordinatorId))).withDeadlineAfter(10, TimeUnit.SECONDS);
        stub.submitJob(request);
        return jobId;
    }

    // Retry submission to the discovered coordinator while leadership settles.
    private int submitToCurrentCoordinator(Set<Integer> voters, String jobType, Object payload) throws Exception {
        long deadline = System.currentTimeMillis() + FAILOVER_BOUND_MS;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            Set<Integer> views = new LinkedHashSet<>();
            for (int id : voters) {
                views.add(workers.get(id).getCurrentCoordinatorId());
            }
            int coordinator = views.size() == 1 ? views.iterator().next() : -1;
            if (coordinator != -1 && workers.containsKey(coordinator)) {
                try {
                    return submitJob(coordinator, jobType, payload);
                } catch (io.grpc.StatusRuntimeException e) {
                    if (e.getStatus().getCode() != io.grpc.Status.Code.FAILED_PRECONDITION) {
                        throw e;
                    }
                    last = e;
                }
            }
            Thread.sleep(250);
        }
        throw new AssertionError("no coordinator would accept the job within the bound", last);
    }

private Object awaitResult(int jobId) throws Exception {
        return answers.get(jobId).get(FAILOVER_BOUND_MS, TimeUnit.MILLISECONDS);
    }

    // Count successful results separately from errors generated during an in-JVM shutdown.
    private int answersDelivered(int jobId) {
        return rejectedAnswers.containsKey(jobId)
                ? (deliveries(jobId) - rejectedAnswers.get(jobId).size())
                : deliveries(jobId);
    }

    // Wait for surviving workers to agree on a coordinator other than the failed one.
    private int awaitCoordinator(Set<Integer> voters, int rejected, long boundMillis) throws Exception {
        long deadline = System.currentTimeMillis() + boundMillis;
        while (System.currentTimeMillis() < deadline) {
            Set<Integer> views = new LinkedHashSet<>();
            for (int id : voters) {
                views.add(workers.get(id).getCurrentCoordinatorId());
            }
            if (views.size() == 1) {
                int agreed = views.iterator().next();
                if (agreed != -1 && agreed != rejected) {
                    return agreed;
                }
            }
            Thread.sleep(100);
        }
        List<String> views = new ArrayList<>();
        StringBuilder events = new StringBuilder();
        for (int id : voters) {
            views.add(id + "=" + workers.get(id).getCurrentCoordinatorId());
            for (com.cs324.common.WorkerEvent event : workers.get(id).getRecentEvents(0)) {
                events.append("\n    w").append(id).append(' ').append(event.kind()).append(' ')
                        .append(event.detail());
            }
        }
        throw new AssertionError("no agreement on a coordinator other than " + rejected
                + " within " + boundMillis + "ms; last views: " + views + "\n  recent events:" + events);
    }

    // Every event of every worker, for failure diagnosis.
    private String clusterFeed() {
        StringBuilder feed = new StringBuilder();
        for (var entry : workers.entrySet()) {
            feed.append("--- w").append(entry.getKey()).append(" ---\n");
            for (com.cs324.common.WorkerEvent event : entry.getValue().getRecentEvents(0)) {
                feed.append(event.kind()).append(' ').append(event.detail()).append('\n');
            }
        }
        return feed.toString();
    }

    // What every worker currently believes, for failure messages.
    private String clusterState() {
        StringBuilder state = new StringBuilder();
        for (var entry : workers.entrySet()) {
            WorkerRuntime worker = entry.getValue();
            int view;
            try {
                view = worker.getCurrentCoordinatorId();
            } catch (RuntimeException e) {
                // A worker that has been unexported cannot be asked; that is the state itself.
                view = -2;
            }
            state.append("w").append(entry.getKey()).append("(coord=").append(view)
                    .append(",shutdown=").append(worker.isShutdown())
                    .append(",port=").append(worker.getProgressPort())
                    .append(") ");
        }
        return state.toString();
    }

    // Wait for the first slice dispatch. With several slices per participant, waiting
    // for every slice can let a fast job finish before the simulated crash.
    private void awaitDispatched(int coordinatorId, int jobId, int expectedSlices) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            long dispatched = workers.get(coordinatorId).getRecentEvents(0).stream()
                    .filter(event -> event.kind().equals(WorkerEvent.TASK_DISPATCHED)
                            && event.detail().startsWith("Job " + jobId + ", task"))
                    .count();
            if (dispatched >= expectedSlices) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("the coordinator did not dispatch job " + jobId
                + " within 30s; feed: " + workers.get(coordinatorId).getRecentEvents(0));
    }

    // Wait for one event kind on any survivor instead of reading a ring that may have rolled over.
    private void awaitEvent(Set<Integer> ids, String kind, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (int id : ids) {
                for (WorkerEvent event : workers.get(id).getRecentEvents(0)) {
                    // Some lifecycle messages are logged as FAILOVER_DETAIL with the
                    // event name in the text, matching the GUI's story feed.
                    if (event.kind().equals(kind) || event.detail().contains(kind)) {
                        return;
                    }
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no survivor recorded " + kind + " within "
                + timeoutMillis + "ms; feed:\n" + recentEvents(ids));
    }

    private String recentEvents(Set<Integer> ids) {
        StringBuilder feed = new StringBuilder();
        for (int id : ids) {
            for (WorkerEvent event : workers.get(id).getRecentEvents(0)) {
                feed.append("w").append(id).append(' ').append(event.kind()).append(' ')
                        .append(event.detail()).append('\n');
            }
        }
        return feed.toString();
    }

    // Kills a worker the way a crashed process looks: no farewell, no deregistration.
    private void crash(int id) throws Exception {
        WorkerRuntime worker = workers.get(id);
        worker.shutdown();

        // The directory still lists the worker: nothing deregistered it, which is the point.
    }

    // Reads the whole progress stream and returns the last snapshot.
    private JobProgress readProgress(int workerId, int jobId) {
        JobProgressServiceGrpc.JobProgressServiceBlockingStub stub = JobProgressServiceGrpc
                .newBlockingStub(channelTo(workers.get(workerId))).withDeadlineAfter(10, TimeUnit.SECONDS);
        Iterator<JobProgress> snapshots = stub.watchJob(WatchJobRequest.newBuilder().setJobId(jobId).build());
        JobProgress last = null;
        while (snapshots.hasNext()) {
            last = snapshots.next();
        }
        return last;
    }

    // How many times the client was called back for one job.
    private int deliveries(int jobId) {
        AtomicInteger count = deliveriesByJob.get(jobId);
        return count == null ? 0 : count.get();
    }

    @Test
    void finishedCheckpointReplaysTheAggregateRatherThanOneSlice() throws Exception {
        WorkerRuntime survivor = addWorker(2);
        int jobId = 41;
        answers.put(jobId, new CompletableFuture<>());
        SubmitJobRequest request = SubmitJobRequest.newBuilder().setJobId(jobId).setJobType("PRIMESUM")
                .setPayload(ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 10)))
                .setClient(ClientEndpoint.newBuilder().setHost("localhost").setPort(clientPort)).build();
        CheckpointLog log = survivor.checkpointLog();
        var owner = com.cs324.registry.proto.WorkerEndpoint.newBuilder().setWorkerId(1).build();
        log.apply(log.acceptedEntry(request, List.of(
                new CheckpointSink.SliceSnapshot(1, ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 5)), owner, 1),
                new CheckpointSink.SliceSnapshot(2, ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(6, 10)), owner, 1)),
                System.currentTimeMillis(), 1, 1));
        log.apply(log.sliceEntry(jobId, 1, null, 1,
                TaskResult.newBuilder().setJobId(jobId).setLongResult(10L).build(), 1, 1));
        log.apply(log.sliceEntry(jobId, 2, null, 1,
                TaskResult.newBuilder().setJobId(jobId).setLongResult(7L).build(), 1, 1));
        log.apply(log.finishedEntry(jobId,
                TaskResult.newBuilder().setJobId(jobId).setLongResult(17L).build(), false, 1, 1));
        survivor.startElection();
        assertEquals(17L, answers.get(jobId).get(5, TimeUnit.SECONDS));
    }

    @Test
    void killingTheCoordinatorMidJobElectsAReplacementThatFinishesTheJobExactlyOnce() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        addWorker(4);
        wireRing();
        Set<Integer> all = Set.of(1, 2, 3, 4);

        workers.get(1).startElection();
        int first = awaitCoordinator(all, -1, 30_000);

        // Pin the arithmetic on a small, independently known answer.
        int smallJob = submitToCurrentCoordinator(all, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, 1000));
        assertEquals(KNOWN_SMALL_SUM, ((Number) awaitResult(smallJob)).longValue());

        // Crash as soon as the first slice starts: with several slices per participant,
        // waiting for every worker to receive one lets a fast job finish before the kill.
        int jobId = submitToCurrentCoordinator(all, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, RANGE_END));
        awaitDispatched(first, jobId, 1);
        assertTrue(!answers.get(jobId).isDone(), "the crash must interrupt an unfinished job");
        crash(first);

        Set<Integer> survivors = new LinkedHashSet<>(all);
        survivors.remove(first);
        int second = awaitCoordinator(survivors, first, FAILOVER_BOUND_MS);
        assertNotEquals(first, second, "a replacement coordinator must be elected");

Object recovered = awaitResult(jobId);
        assertNotNull(recovered, "the client must still receive an answer");
        assertTrue(recovered instanceof Long, "the client must receive a sum, not an error: " + recovered
                + " | crashed=" + first + " replacement=" + second + " | " + clusterState());
        // Use an independent expected sum to detect missing, duplicated or stale slice results.
        assertEquals(ORACLE_SUM, ((Number) recovered).longValue(),
                "the recovered job must produce the exact answer for its input");
        // Count successful callbacks separately from errors emitted during an in-JVM shutdown.
        assertEquals(1, answersDelivered(jobId),
                () -> "the client must be called back exactly once for the failed job; feed:\n" + clusterFeed());
        // One coordinator rebuilds the job and owns it, so no survivor rebuilds it in parallel.
        long rebuilds = workers.values().stream()
                .flatMap(worker -> worker.getRecentEvents(0).stream())
                .filter(event -> event.detail().contains("Job " + jobId + " restored on coordinator"))
                .count();
        assertEquals(1, rebuilds, () -> "only one coordinator may rebuild the failed job; feed:\n" + clusterFeed());
        assertNotNull(workers.get(second).checkpointLog().job(jobId));
        assertNotEquals(first, workers.get(second).checkpointLog().job(jobId).acceptedBy(),
                "the recovered job is no longer owned by the crashed coordinator");
        assertTrue(rejectedAnswers.getOrDefault(jobId, List.of()).size() <= 1,
                "a dying coordinator may report its own failure at most once");

        JobProgress progress = readProgress(second, jobId);
        assertNotNull(progress, "the new coordinator must publish progress for the recovered job");
        assertTrue(progress.getFinished(), "the recovered job must reach 100% on the new coordinator");
        assertEquals(RANGE_END, progress.getWorkersList().stream()
                        .mapToLong(WorkerProgress::getCompletedUnits).sum(),
                "every input value must be accounted for exactly once");

        // The new coordinator is a working coordinator, not just a replacement leader.
        int afterJob = submitToCurrentCoordinator(survivors, "PRIMECOUNT", new int[] {2, 3, 4, 5, 8, 11});
        assertEquals(4, ((Number) awaitResult(afterJob)).intValue());
        assertEquals(1, answersDelivered(afterJob));
    }

    @Test
    void aSurvivingWorkerReportsTheFailureAndTheRecoveryInItsEventFeed() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        wireRing();
        Set<Integer> all = Set.of(1, 2, 3);

        workers.get(1).startElection();
        int first = awaitCoordinator(all, -1, 30_000);

        int jobId = submitToCurrentCoordinator(all, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, RANGE_END));
        awaitDispatched(first, jobId, 1);
        assertTrue(!answers.get(jobId).isDone(), "the crash must interrupt an unfinished job");
        crash(first);
        // Only survivors can be polled: a crashed worker's view is frozen at the old leader.
        Set<Integer> survivors = new LinkedHashSet<>(all);
        survivors.remove(first);
        int second = awaitCoordinator(survivors, first, FAILOVER_BOUND_MS);

        // The ring keeps only recent events. With several slices per worker, waiting for
        // the result first can evict the early recovery events, so observe each one as it appears.
        awaitEvent(survivors, WorkerEvent.COORDINATOR_FAILED, FAILOVER_BOUND_MS);
        awaitEvent(survivors, WorkerEvent.FAILOVER_STARTED, FAILOVER_BOUND_MS);
        awaitEvent(survivors, WorkerEvent.JOB_RECOVERED, FAILOVER_BOUND_MS);
        awaitEvent(survivors, WorkerEvent.CHECKPOINT_REPLICATED, FAILOVER_BOUND_MS);
        awaitResult(jobId);
        assertNotEquals(first, second);
    }

    @Test
    void aStarTopologyCanReplaceItsFailedHub() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        // Leaves 1 and 2 are linked only to the hub. Killing the hub leaves each
        // survivor with a dead edge, which repair must replace before an election
        // can reach the other survivor and a majority.
        workers.get(1).connectAsNeighbour(3, "localhost", contactPort);
        workers.get(3).connectAsNeighbour(1, "localhost", contactPort);
        workers.get(2).connectAsNeighbour(3, "localhost", contactPort);
        workers.get(3).connectAsNeighbour(2, "localhost", contactPort);

        workers.get(3).startElection();
        assertEquals(3, awaitCoordinator(Set.of(1, 2, 3), -1, 30_000));

        crash(3);
        int second = awaitCoordinator(Set.of(1, 2), 3, FAILOVER_BOUND_MS);
        assertNotEquals(3, second);
    }

    @Test
    void aFinishedJobIsNotRebuiltByTheNextElection() throws Exception {
        addWorker(1);
        addWorker(2);
        addWorker(3);
        wireRing();
        Set<Integer> all = Set.of(1, 2, 3);

        workers.get(1).startElection();
        int first = awaitCoordinator(all, -1, 30_000);

        int jobId = submitToCurrentCoordinator(all, "MAX", new int[] {3, 9, 4, 9, -2});
        assertEquals(9, ((Number) awaitResult(jobId)).intValue());
        assertEquals(1, answersDelivered(jobId));

        // Keep finished-job markers through the next election to prevent recomputing completed work.
        workers.get(2).startElection();
        int second = awaitCoordinator(all, -1, 30_000);
        assertNotEquals(-1, second);
        Thread.sleep(2_000);

        assertEquals(1, answersDelivered(jobId), "a finished job is never delivered twice");
        assertTrue(answers.get(jobId).isDone() && !answers.get(jobId).isCompletedExceptionally());
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
        for (ManagedChannel channel : channels) {
            channel.shutdownNow();
        }
        for (Server server : servers) {
            server.shutdownNow();
        }
        for (Server server : servers) {
            server.awaitTermination(5, TimeUnit.SECONDS);
        }


    }
}
