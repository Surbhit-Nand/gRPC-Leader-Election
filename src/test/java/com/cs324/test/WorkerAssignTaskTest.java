package com.cs324.test;

import com.cs324.worker.WorkerRemoteImpl;
import com.cs324.worker.WorkerTaskCalculator;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

// Step 7 of the worker test plan: assignTask().
// assignTask is fire-and-return: the worker computes on a pool thread and calls back later.
// These tests pin the three assignment calculations and prove the result crosses RMI.
// The JAC is deliberately unasserted here; it counts coordinator-side assignments.
class WorkerAssignTaskTest {

    private static final int TEST_RMI_PORT = 19806;
    private static final String HOST = "localhost";
    private static final int COORDINATOR_ID = 9;
    private static final int THIS_WORKER_ID = 1;
    private static final long AWAIT_TIMEOUT_MS = 5_000;

    // Shared across tests; recreating an RMI registry on one port between tests is flaky.
    private static Registry registry;

    // A real worker, so the background compute path under test is the production one.
    private WorkerRemoteImpl worker;
    // A recording double stands in for the coordinator, so results are observable.
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

        // Peer and coordinator are the same stub, so the callback has a live target.
        worker.connectAsNeighbour(COORDINATOR_ID, HOST, TEST_RMI_PORT);
        worker.receiveCoordinatorMessage(COORDINATOR_ID, 1L);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(worker);
        unexportQuietly(coordinator);
    }

    private void unexportQuietly(Remote remoteObject) {
        if (remoteObject == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
            // already unexported, nothing to do
        }
    }

    // Polls the double until the callback lands, because the compute runs on another thread.
    private String awaitReportedResult(int jobId) throws Exception {
        // A deadline rather than a join, because the work was handed to another thread.
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            if (coordinator.receivedResult(jobId, THIS_WORKER_ID)) {
                return coordinator.lastReportedResult(jobId, THIS_WORKER_ID);
            }
            Thread.sleep(20);
        }

        fail("assignTask returned but never reported a result for job " + jobId + " within "
                + AWAIT_TIMEOUT_MS + "ms - the background task either never ran or could not reach the "
                + "coordinator; recorded: " + coordinator.calls());
        return null;
    }

    @Test
    void reportsTheMaximumOfTheAssignedNumbers() throws Exception {
        worker.assignTask(1, "MAX", new int[]{4, 11, 2, 19, 8});

        assertEquals("19", awaitReportedResult(1),
                "MAX(4, 11, 2, 19, 8) is 19");
    }

    @Test
    void reportsTheSumOfPrimesOverTheAssignedRange() throws Exception {
        worker.assignTask(2, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, 10));

        assertEquals("17", awaitReportedResult(2),
                "PRIMESUM(1, 10) covers 2, 3, 5 and 7, which sum to 17");
    }

    @Test
    void reportsHowManyPrimesAreInTheAssignedList() throws Exception {
        worker.assignTask(3, "PRIMECOUNT", new int[]{2, 8, 7, 4, 11, 10});

        assertEquals("3", awaitReportedResult(3),
                "PRIMECOUNT(2, 8, 7, 4, 11, 10) sees 2, 7 and 11");
    }

    // Regression: a two-element payload is data, never a range to iterate.
    @Test
    void maxTreatsTwoNumbersAsDataNotAsARange() throws Exception {
        worker.assignTask(4, "MAX", new int[]{5, 3});

        assertEquals("5", awaitReportedResult(4),
                "a two element MAX payload is still just two numbers - reading it as a range 5..3 would "
                        + "produce an empty list and a nonsense answer");
    }

    @Test
    void completedTasksAreNotLeftCountedAsActive() throws Exception {
        // A leak guard, not a reproduction: the racy interleaving is impractical to force.
        // A task that finished before it was recorded would strand its active entry.
        final int jobs = 50;
        for (int job = 1; job <= jobs; job++) {
            worker.assignTask(1000 + job, "MAX", new int[]{job, job + 1});
            awaitReportedResult(1000 + job);
        }

        // The count settles asynchronously, so poll instead of reading it once.
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && worker.getActiveTaskCount() != 0) {
            Thread.sleep(20);
        }

        assertEquals(0, worker.getActiveTaskCount(),
                jobs + " tasks have all reported their results, so none of them may still be listed as "
                        + "running; a task that finished before it was recorded leaves its entry behind");
    }
}
