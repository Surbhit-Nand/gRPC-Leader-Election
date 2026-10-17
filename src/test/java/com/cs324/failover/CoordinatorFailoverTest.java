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
import com.cs324.worker.WorkerRemoteImpl;
import com.cs324.worker.WorkerTaskCalculator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coordinator failure over real TCP: four workers, a real directory, a real client callback.
 *
 * <p>The job is made long on purpose - each worker's share takes several seconds - so the
 * coordinator is guaranteed to be killed with work genuinely in flight, rather than relying on
 * winning a race. Killing it is a crash, not a clean exit: the server is dropped and the RMI
 * object unexported, but the directory still lists the worker, exactly as it would for a
 * process that died without deregistering.
 *
 * <p>The assertions are the ones the assignment asks for: a replacement coordinator is elected
 * within a bound, the job finishes with the same answer it would have had, the client is
 * called back exactly once, and the recovered job's progress stream reaches 100%.
 */
class CoordinatorFailoverTest {

    /**
     * Four workers each check 20M numbers, which takes about ten seconds per share: far longer
     * than the few seconds the cluster needs to notice a dead coordinator, so the kill is
     * guaranteed to land mid-job rather than winning a race.
     */
    private static final int RANGE_END = 80_000_000;
    /**
     * Sum of the primes up to {@link #RANGE_END}, computed independently with the plain
     * calculator ({@code WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(1, 80_000_000))}).
     * Comparing against an oracle rather than against another run of the same system is what
     * makes a dropped or double-counted slice fail the test.
     */
    private static final long ORACLE_SUM = 180_946_446_237_951L;
    /** Small, independently known answer used to pin the arithmetic in this same cluster. */
    private static final long KNOWN_SMALL_SUM = 76_127L;
    private static final long FAILOVER_BOUND_MS = 90_000;

    private final Map<Integer, WorkerRemoteImpl> workers = new LinkedHashMap<>();
    private final List<Server> servers = new ArrayList<>();
    private final List<Remote> exported = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final Map<Integer, CompletableFuture<Object>> results = new ConcurrentHashMap<>();
    // Counted per job: "delivered exactly once" is a claim about one job, not about the run.
    private final Map<Integer, AtomicInteger> deliveriesByJob = new ConcurrentHashMap<>();
    private final AtomicInteger nextJobId = new AtomicInteger(1);
    private Registry rmiRegistry;
    private int rmiPort;
    private int bootstrapPort;
    private int clientPort;

    @BeforeEach
    void setUp() throws Exception {
        workers.clear();
        servers.clear();
        exported.clear();
        channels.clear();
        results.clear();
        deliveriesByJob.clear();
        nextJobId.set(1);

        Server bootstrap = ServerBuilder.forPort(0)
                .addService(new BootstrapGrpcService(new BootstrapRegistryImpl())).build().start();
        servers.add(bootstrap);
        bootstrapPort = bootstrap.getPort();

        Server callbacks = ServerBuilder.forPort(0).addService(new ClientResultService((jobId, value) -> {
            deliveriesByJob.computeIfAbsent(jobId, key -> new AtomicInteger()).incrementAndGet();
            results.computeIfAbsent(jobId, key -> new CompletableFuture<>()).complete(value);
        })).build().start();
        servers.add(callbacks);
        clientPort = callbacks.getPort();

        try (ServerSocket reservation = new ServerSocket(0)) {
            rmiPort = reservation.getLocalPort();
        }
        rmiRegistry = LocateRegistry.createRegistry(rmiPort);
    }

    /** One worker with its own gRPC listener, published through the directory as in production. */
    private WorkerRemoteImpl addWorker(int id) throws Exception {
        WorkerRemoteImpl worker = new WorkerRemoteImpl(id);
        rmiRegistry.rebind("Worker" + id, worker);
        exported.add(worker);
        workers.put(id, worker);
        worker.startProgressServer();
        registry().register(RegisterRequest.newBuilder()
                .setWorkerId(id).setHost("localhost").setPort(rmiPort).build());
        worker.enableGrpcJobs("localhost", bootstrapPort, "localhost", rmiPort);
        return worker;
    }

    private RegistryServiceGrpc.RegistryServiceBlockingStub registry() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", bootstrapPort)
                .usePlaintext().build();
        channels.add(channel);
        return RegistryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    /** A ring: everyone is two hops from everyone, so no topology decides the outcome. */
    private void wireRing() throws Exception {
        int[] ids = workers.keySet().stream().mapToInt(Integer::intValue).toArray();
        for (int i = 0; i < ids.length; i++) {
            workers.get(ids[i]).connectAsNeighbour(ids[(i + 1) % ids.length], "localhost", rmiPort);
            workers.get(ids[(i + 1) % ids.length]).connectAsNeighbour(ids[i], "localhost", rmiPort);
        }
    }

    private ManagedChannel channelTo(WorkerRemoteImpl worker) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", worker.getProgressPort()).usePlaintext().build();
        channels.add(channel);
        return channel;
    }

    private int submitJob(int coordinatorId, String jobType, Object payload) {
        int jobId = nextJobId.getAndIncrement();
        results.put(jobId, new CompletableFuture<>());
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

    /**
     * Submits to whoever the cluster currently believes leads, retrying while it settles.
     *
     * <p>A refusal is the designed answer to "leadership moved": the job id is released and the
     * client is expected to retry. After a failover the cluster can still be converging, so a
     * test that hard-codes a worker id would be asserting that leadership never moves again.
     */
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
        return results.get(jobId).get(FAILOVER_BOUND_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Waits until the given workers agree on one coordinator that is not the rejected one.
     *
     * <p>Agreement alone is not enough right after a crash: every survivor still names the dead
     * worker for as long as it takes to notice, and returning that view would let the test pass
     * without a failover having happened at all.
     */
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

    /**
     * Waits until the coordinator has actually dispatched every slice of a job.
     *
     * <p>A submission returns as soon as the work is queued, so killing a coordinator the
 * <em>instant</em> after the ack tests a worker that had not started working yet. Waiting for
 * the dispatch records makes "killed mid-job" mean what it says: every slice is out, and the
 * calculations are genuinely running when the process goes.
 */
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
        throw new AssertionError("the coordinator did not dispatch every slice of job " + jobId
                + " within 30s; feed: " + workers.get(coordinatorId).getRecentEvents(0));
    }

    /** Kills a worker the way a crashed process looks: no farewell, no deregistration. */
    private void crash(int id) throws Exception {
        WorkerRemoteImpl worker = workers.get(id);
        worker.shutdown();
        UnicastRemoteObject.unexportObject(worker, true);
        for (String name : rmiRegistry.list()) {
            if (name.equals("Worker" + id)) {
                rmiRegistry.unbind(name);
            }
        }
        // The directory still lists the worker: nothing deregistered it, which is the point.
    }

    /** Reads the whole progress stream and returns the last snapshot. */
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

    /** How many times the client was called back for one job. */
    private int deliveries(int jobId) {
        AtomicInteger count = deliveriesByJob.get(jobId);
        return count == null ? 0 : count.get();
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

        // A long job: each share takes seconds, so the kill lands with work in flight.
        int jobId = submitToCurrentCoordinator(all, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, RANGE_END));
        awaitDispatched(first, jobId, workers.size());
        crash(first);

        Set<Integer> survivors = new LinkedHashSet<>(all);
        survivors.remove(first);
        int second = awaitCoordinator(survivors, first, FAILOVER_BOUND_MS);
        assertNotEquals(first, second, "a replacement coordinator must be elected");

        Object recovered = awaitResult(jobId);
        assertNotNull(recovered, "the client must still receive an answer");
        assertTrue(recovered instanceof Long, "the client must receive a sum, not an error: " + recovered);
        // Compared against an independently computed oracle: a slice that was dropped, counted
        // twice, or recomputed from a stale input all produce a different sum.
        assertEquals(ORACLE_SUM, ((Number) recovered).longValue(),
                "the recovered job must produce the exact answer for its input");
        assertEquals(1, deliveries(jobId), "the client must be called back exactly once for the failed job");

        JobProgress progress = readProgress(second, jobId);
        assertNotNull(progress, "the new coordinator must publish progress for the recovered job");
        assertTrue(progress.getFinished(), "the recovered job must reach 100% on the new coordinator");
        assertEquals(RANGE_END, progress.getWorkersList().stream()
                        .mapToLong(WorkerProgress::getCompletedUnits).sum(),
                "every input value must be accounted for exactly once");

        // The new coordinator is a working coordinator, not just a replacement leader.
        int afterJob = submitToCurrentCoordinator(survivors, "PRIMECOUNT", new int[] {2, 3, 4, 5, 8, 11});
        assertEquals(4, ((Number) awaitResult(afterJob)).intValue());
        assertEquals(1, deliveries(afterJob));
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
        awaitDispatched(first, jobId, workers.size());
        crash(first);
        // Only survivors can be polled: a crashed worker's view is frozen at the old leader.
        Set<Integer> survivors = new LinkedHashSet<>(all);
        survivors.remove(first);
        int second = awaitCoordinator(survivors, first, FAILOVER_BOUND_MS);
        awaitResult(jobId);

        // The feed is what the demo shows, so the whole story has to be in it.
        StringBuilder feed = new StringBuilder();
        for (int id : survivors) {
            for (com.cs324.common.WorkerEvent event : workers.get(id).getRecentEvents(0)) {
                feed.append(event.kind()).append(' ').append(event.detail()).append('\n');
            }
        }
        String text = feed.toString();
        assertTrue(text.contains(WorkerEvent.COORDINATOR_FAILED),
                "a survivor must record that the coordinator failed");
        assertTrue(text.contains(WorkerEvent.FAILOVER_STARTED),
                "a survivor must record that it started a failover");
        assertTrue(text.contains(WorkerEvent.JOB_RECOVERED),
                "the new coordinator must record the job it took over");
        assertTrue(text.contains(WorkerEvent.CHECKPOINT_REPLICATED),
                "the coordinator must record that it replicated job state");
        assertNotEquals(first, second);
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
        assertEquals(1, deliveries(jobId));

        // The finished job is still in every replica as a tombstone. A new election triggers a
        // takeover that merges those replicas, and must skip it rather than run the work again.
        workers.get(2).startElection();
        int second = awaitCoordinator(all, -1, 30_000);
        assertNotEquals(-1, second);
        Thread.sleep(2_000);

        assertEquals(1, deliveries(jobId), "a finished job is never delivered twice");
        assertTrue(results.get(jobId).isDone() && !results.get(jobId).isCompletedExceptionally());
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
        for (ManagedChannel channel : channels) {
            channel.shutdownNow();
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