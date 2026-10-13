package com.cs324.test;

import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

// Multi-hop election over real RMI in one JVM: every reachable worker must settle on ONE
// coordinator, chosen by lowest JAC and then by highest id.
// The triangle is deliberate - two of its nodes forward the election to each other, so each
// gets a duplicate it must answer rather than drop, or those two wait on each other forever.
// The awaitAgreement timeout turns that deadlock into a failure instead of a hung run.
class WorkerElectionConvergenceTest {

    private static final int TEST_RMI_PORT = 19804;
    private static final String HOST = "localhost";
    private static final long CONVERGENCE_TIMEOUT_MS = 5_000;

    private static Registry registry;

    // Five workers exist for every test; each topology helper wires a subset of them.
    private WorkerRemoteImpl w1;
    private WorkerRemoteImpl w2;
    private WorkerRemoteImpl w3;
    private WorkerRemoteImpl w4;
    private WorkerRemoteImpl w5;

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
        // Nothing is wired or seeded here: each test picks its own topology, so no leftover
        // neighbour or JAC can decide the election on the test's behalf.
        w1 = new WorkerRemoteImpl(1);
        w2 = new WorkerRemoteImpl(2);
        w3 = new WorkerRemoteImpl(3);
        w4 = new WorkerRemoteImpl(4);
        w5 = new WorkerRemoteImpl(5);

        registry.rebind("Worker1", w1);
        registry.rebind("Worker2", w2);
        registry.rebind("Worker3", w3);
        registry.rebind("Worker4", w4);
        registry.rebind("Worker5", w5);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Every binding must go, or the next test resolves a worker left over from this one.
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(w1);
        unexportQuietly(w2);
        unexportQuietly(w3);
        unexportQuietly(w4);
        unexportQuietly(w5);
    }

    private void unexportQuietly(Remote remoteObject) {
        if (remoteObject == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
            // Already unexported by the test itself, so a second attempt is not an error.
        }
    }

    // Only these five ids exist, so a test naming another one is a bug, not a topology.
    private WorkerRemoteImpl worker(int id) {
        return switch (id) {
            case 1 -> w1;
            case 2 -> w2;
            case 3 -> w3;
            case 4 -> w4;
            case 5 -> w5;
            default -> throw new IllegalArgumentException("unknown worker " + id);
        };
    }

    // One edge per call, so both directions of a link are wired explicitly.
    private void connect(int fromId, int toId) throws Exception {
        worker(fromId).connectAsNeighbour(toId, HOST, TEST_RMI_PORT);
    }

    // 1-2, 2-3, 3-1: every node has two neighbours, and the graph contains a cycle.
    private void wireTriangle() throws Exception {
        connect(1, 2);
        connect(2, 1);
        connect(2, 3);
        connect(3, 2);
        connect(3, 1);
        connect(1, 3);
    }

    // 1-2-3: a plain line, so convergence cannot depend on a cycle being present.
    private void wireChain() throws Exception {
        connect(1, 2);
        connect(2, 1);
        connect(2, 3);
        connect(3, 2);
    }

    // 1<->2, 1<->3, 2<->3 with a 3<->4<->5 tail: the only shape where the cycle and the dead
    // ends sit on opposite sides of one node. Worker 3 forwards both ways, so the winning candidate
    // has to be carried into a branch whose members are the strongest in the graph, applying both
    // rules to candidates that never met each other.
    private void wireTriangleWithTwoWorkerTail() throws Exception {
        connect(1, 2);
        connect(2, 1);
        connect(1, 3);
        connect(3, 1);
        connect(2, 3);
        connect(3, 2);
        connect(3, 4);
        connect(4, 3);
        connect(4, 5);
        connect(5, 4);
    }

    // Reflection is deliberate: there is no public way to arrange JAC values yet, and driving it
    // through assignTask() would tie these tests to wherever the increment currently lives.
    private static void seedJobAllocationCounter(WorkerRemoteImpl worker, int value) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("jobAllocationCounter");
        field.setAccessible(true);
        ((AtomicInteger) field.get(worker)).set(value);
    }

    // Polls the three workers wired by the triangle and chain helpers.
    private void awaitAgreement(int expectedCoordinatorId) throws Exception {
        awaitAgreement(expectedCoordinatorId, w1, w2, w3);
    }

    // The participating set is a parameter so any topology reuses one convergence check. Voting
    // is asynchronous, so one read can catch a worker mid-flight: poll to the deadline instead.
    private void awaitAgreement(int expectedCoordinatorId, WorkerRemoteImpl... participants) throws Exception {
        long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            boolean agreed = true;
            // Every participant must name the expected coordinator, not just one of them.
            for (WorkerRemoteImpl participant : participants) {
                if (participant.getCurrentCoordinatorId() != expectedCoordinatorId) {
                    agreed = false;
                    break;
                }
            }
            if (agreed) {
                return;
            }
            Thread.sleep(20);
        }

        fail("the election did not converge on coordinator " + expectedCoordinatorId + " within "
                + CONVERGENCE_TIMEOUT_MS + "ms - if the workers are stuck at -1 or disagree, the echo phase never "
                + "terminated (a duplicate that was dropped instead of answered does exactly this). Got: "
                + describe(participants));
    }

    // Renders "w1=3, w2=5, ..." for the failure message, the only diagnosis available here.
    private String describe(WorkerRemoteImpl... participants) throws RemoteException {
        StringBuilder rendered = new StringBuilder();
        for (WorkerRemoteImpl participant : participants) {
            if (rendered.length() > 0) {
                rendered.append(", ");
            }
            rendered.append("w").append(participant.getId())
                    .append("=").append(participant.getCurrentCoordinatorId());
        }
        return rendered.toString();
    }

    @Test
    void aTriangleConvergesOnOneCoordinatorAndTiesGoToTheHighestId() throws Exception {
        // Smallest graph containing a cycle, so a failure here is not a topology artefact.
        wireTriangle();

        w1.startElection();

        // All three start on JAC 0, so the id tie-break decides: worker 3 wins.
        awaitAgreement(3);
    }

    @Test
    void theWorkerWithTheLowestJobAllocationCounterWins() throws Exception {
        wireTriangle();
        // JAC 0, 5 and 9: the highest id must not be able to outrank the lowest JAC.
        seedJobAllocationCounter(w1, 0);
        seedJobAllocationCounter(w2, 5);
        seedJobAllocationCounter(w3, 9);

        // Worker 3 has the highest id, but the lowest JAC must take priority.
        w2.startElection();

        awaitAgreement(1);
    }

    @Test
    void whenLowestJacIsTiedTheHigherWorkerIdWins() throws Exception {
        wireTriangle();
        // JAC 2, 1, 1: only the two lowest are in the running, and 3 outranks 2 on id.
        seedJobAllocationCounter(w1, 2);
        seedJobAllocationCounter(w2, 1);
        seedJobAllocationCounter(w3, 1);

        w1.startElection();

        // Workers 2 and 3 share the lowest JAC, so the tie must break on the higher id. This
        // is the only test that applies both rules in sequence on a single election.
        awaitAgreement(3);
    }

    @Test
    void aChainTopologyWithNoCycleAlsoConverges() throws Exception {
        // A line has no cycle, so termination cannot come from duplicate suppression.
        wireChain();

        w2.startElection();

        awaitAgreement(3);
    }

    @Test
    void aSecondElectionIsNotMistakenForTheFirst() throws Exception {
        // Baseline wave first, so the second has a settled cluster to be compared against.
        wireTriangle();

        w1.startElection();
        awaitAgreement(3);

        // A fresh message id, or this would only re-test the duplicate-suppression path.
        w1.startElection();
        awaitAgreement(3);

        assertEquals(3, w2.getCurrentCoordinatorId(),
                "a new election must be applied normally rather than discarded as a repeat");
    }

    @Test
    void fullElectionConvergesAcrossMixedFiveWorkerTopology() throws Exception {
        wireTriangleWithTwoWorkerTail();

        // JAC 4, 2, 3, 1, 1: the tail holds the lowest, and the winner is a leaf past the branch.
        seedJobAllocationCounter(w1, 4);
        seedJobAllocationCounter(w2, 2);
        seedJobAllocationCounter(w3, 3);
        seedJobAllocationCounter(w4, 1);
        seedJobAllocationCounter(w5, 1);

        // Origin is worker 2, so the wave must cross the triangle and the tail to be known.
        w2.startElection();

        // Workers 4 and 5 share the graph's lowest JAC at 1, which eliminates 1, 2 and 3 outright;
        // the id rule then picks 5. Worker 5 is a leaf, so a wave that stops at the branch point
        // (worker 3) shows up here as worker 5 never winning at all.
        awaitAgreement(5, w1, w2, w3, w4, w5);
    }
}
