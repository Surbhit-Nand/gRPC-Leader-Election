package com.cs324.test;

import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// Concurrency / thread-safety tests for worker-side job execution, kept clear of
// election semantics: distinct jobs must not block each other, duplicate
// assignments must collapse to one execution, and per-worker results for a
// single job must all survive a concurrent update.
// JAC note: the spec counts assignments a coordinator issues to others, so a
// worker RECEIVING assignTask() leaves its own counter at zero - that is moved
// in submitJob() on the coordinator side.
class WorkerConcurrencyTest {

    // Ports are unique per class, so the suites can run side by side.
    private static final int TEST_RMI_PORT = 19807;
    private static final String HOST = "localhost";
    private static final int COORDINATOR_ID = 9;
    private static final int THIS_WORKER_ID = 1;
    private static final long AWAIT_TIMEOUT_MS = 10_000;

    // Shared by every test here, created once so none pays registry startup.
    private static Registry registry;

    // The worker under test: id 1, with one recorder acting as its coordinator.
    private WorkerRemoteImpl worker;
    // Stands in for the coordinator, recording every result reported to it.
    private RecordingWorkerRemote coordinator;

    @BeforeAll
    static void startRegistry() throws RemoteException {
        registry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @BeforeEach
    void setUp() throws Exception {
        worker = new WorkerRemoteImpl(THIS_WORKER_ID);
        coordinator = new RecordingWorkerRemote(COORDINATOR_ID);

        registry.rebind("Worker" + COORDINATOR_ID, coordinator);

        worker.connectAsNeighbour(COORDINATOR_ID, HOST, TEST_RMI_PORT);
        // Without a known coordinator, results would fall back to reporting to self.
        worker.receiveCoordinatorMessage(COORDINATOR_ID, 777L);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            try {
                registry.unbind(name);
            } catch (Exception ignored) {
            }
        }
        if (worker != null) {
            worker.shutdown();
        }
        unexportQuietly(worker);
        unexportQuietly(coordinator);
    }

    // RMI export is a side effect of construction, so every test unexports its doubles.
    private void unexportQuietly(Remote remoteObject) {
        if (remoteObject == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
        }
    }

    // Polls because the compute runs on a pool thread; a fixed sleep would be flaky.
    private void awaitReportedResult(int jobId) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (coordinator.receivedResult(jobId, THIS_WORKER_ID)) {
                return;
            }
            Thread.sleep(20);
        }
        fail("job " + jobId + " never reported within " + AWAIT_TIMEOUT_MS + "ms; recorded: "
                + coordinator.calls());
    }

    @Test
    // Head-of-line blocking: 20 distinct jobs all finish, none queued behind another.
    void concurrentDistinctJobsAllCompleteWithoutBlockingEachOther() throws Exception {
        int jobs = 20;
        ExecutorService submitters = Executors.newFixedThreadPool(8);
        try {
            for (int j = 0; j < jobs; j++) {
                final int jobId = 1000 + j;
                // assignTask must return at once; the slice is computed on the worker pool.
                submitters.submit(() -> {
                    try {
                        worker.assignTask(jobId, "MAX", new int[]{jobId, jobId + 1, jobId - 1});
                    } catch (RemoteException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            submitters.shutdown();
            assertTrue(submitters.awaitTermination(5, TimeUnit.SECONDS), "submitters hung");
        } finally {
            submitters.shutdownNow();
        }

        // Fire-and-return: all 20 submissions complete first, then each result reports back.
        for (int j = 0; j < jobs; j++) {
            awaitReportedResult(1000 + j);
        }
        for (int j = 0; j < jobs; j++) {
            assertEquals(String.valueOf(1000 + j + 1),
                    coordinator.lastReportedResult(1000 + j, THIS_WORKER_ID),
                    "wrong MAX result for job " + (1000 + j));
        }
        // JAC is a coordinator-only counter, so a receiver's own counter stays at zero.
        assertEquals(0, worker.getJobAllocationCounter(),
                "a worker that receives assignTask() must not increment its own JAC; "
                        + "JAC counts coordinator-side assignments only");
    }

    @Test
    // Regression: one jobId assigned concurrently must be computed and reported once.
    void duplicateConcurrentSubmissionsOfTheSameJobExecuteOnlyOnce() throws Exception {
        int duplicates = 10;
        int jobId = 2000;
        // One latch for all submitters, so the claim race is as wide as possible.
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService submitters = Executors.newFixedThreadPool(duplicates);
        try {
            for (int i = 0; i < duplicates; i++) {
                submitters.submit(() -> {
                    try {
                        start.await();
                        worker.assignTask(jobId, "MAX", new int[]{4, 11, 2, 19, 8});
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            start.countDown(); // release all submitters at once for maximum contention
            submitters.shutdown();
            assertTrue(submitters.awaitTermination(5, TimeUnit.SECONDS), "submitters hung");
        } finally {
            submitters.shutdownNow();
        }

        // Exactly one report is due; waiting past it would expose a stray duplicate.
        awaitReportedResult(jobId);
        Thread.sleep(300);

        long reports = coordinator.calls().stream()
                .filter(c -> c.startsWith("result|" + jobId + "|" + THIS_WORKER_ID + "|"))
                .count();
        assertEquals(1L, reports,
                "duplicate assignTask calls must collapse to one execution, otherwise work is "
                        + "duplicated; recorded: " + coordinator.calls());
        assertEquals(0, worker.getJobAllocationCounter(),
                "a receiver of assignTask() must not count the job; JAC is coordinator-only");
        assertEquals("19", coordinator.lastReportedResult(jobId, THIS_WORKER_ID));
    }

    @Test
    // The per-job lock must let 8 workers report one job concurrently without loss.
    void concurrentResultsForTheSameJobFromDifferentWorkersAreAllRetained() throws Exception {
        int jobId = 3000;
        int reporters = 8;
        // Same latch: every reporter races into the same job at the same moment.
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(reporters);
        try {
            for (int w = 0; w < reporters; w++) {
                final int workerId = 50 + w;
                pool.submit(() -> {
                    try {
                        start.await();
                        worker.reportTaskResult(jobId, workerId, "r-" + workerId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "reporters hung");
        } finally {
            pool.shutdownNow();
        }

        // The (jobId, workerId) composite key is what stops results overwriting each other.
        Map<Integer, Object> results = worker.getJobResults(jobId);
        assertEquals(reporters, results.size(),
                "concurrent per-worker results for one job must not overwrite each other: " + results);
        for (int w = 0; w < reporters; w++) {
            assertEquals("r-" + (50 + w), String.valueOf(results.get(50 + w)));
        }
    }

    @Test
    // Election ids come from an AtomicLong, so a race cannot hand two waves the same id.
    void concurrentElectionsProduceUniqueMessageIds() throws Exception {
        int elections = 10;
        ExecutorService pool = Executors.newFixedThreadPool(elections);
        // Collection is shared across pool threads, so it needs its own synchronisation.
        Set<Long> ids = java.util.Collections.synchronizedSet(new HashSet<>());
        try {
        // All 10 threads block on one latch, so the wave starts are as simultaneous as possible.
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < elections; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        ids.add(worker.startElection());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "election starters hung");
        } finally {
            pool.shutdownNow();
        }
        assertEquals(elections, ids.size(), "every election wave needs a unique message ID, got: " + ids);
    }
}
