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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// receiveEchoMessage() and the propagation it drives: this is where a wave ends.
// A node reports upward only once every child has answered, and the initiator,
// having no parent, is the node that declares and announces the winner.
// Termination is a local condition, never a timeout, and a once-only latch keeps a
// repeated answer from being propagated a second time.
class WorkerEchoTest {

    private static final int TEST_RMI_PORT = 19803;
    private static final String HOST = "localhost";
    private static final long ELECTION_ID = 77L;

    private static Registry registry;

    // Node 1 sits between a parent it must report to and children that owe it echoes.
    private WorkerRemoteImpl worker;      // id 1 - the node under test
    private RecordingWorkerRemote parent; // id 2 - who the echo must be sent up to
    private RecordingWorkerRemote child;  // id 3 - who owes this node an echo
    private RecordingWorkerRemote otherChild; // id 4 - a second child, for the two-child case

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
        worker = new WorkerRemoteImpl(1);
        parent = new RecordingWorkerRemote(2);
        child = new RecordingWorkerRemote(3);

        // Bindings let the worker resolve neighbours by name, as a real join would.
        registry.rebind("Worker2", parent);
        registry.rebind("Worker3", child);

        worker.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
        worker.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(worker);
        unexportQuietly(parent);
        unexportQuietly(child);
        unexportQuietly(otherChild);
    }

    // RMI export is a side effect of construction, so every test unexports its doubles.
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

    // Echoes are counted from the parent's call log, so a second send becomes visible.
    private static long countOf(List<String> calls, String prefix) {
        return calls.stream().filter(call -> call.startsWith(prefix)).count();
    }

    // Winner announcements are checked on the child, which has no parent to report to.
    private static boolean receivedCoordinatorFor(List<String> calls, int coordinatorId) {
        return calls.stream().anyMatch(call -> call.startsWith("coordinator|" + coordinatorId + "|"));
    }

    // Puts this worker mid-election: parent 2 has forwarded, child 3 still owes an echo.
    private void receiveElectionFromParent() throws Exception {
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID);
    }

    @Test
    // With its only child answered, this node's subtree is complete and must report up.
    void theLastEchoIsForwardedUpToTheParent() throws Exception {
        receiveElectionFromParent();

        worker.receiveEchoMessage(ELECTION_ID, 3, 3, 0);

        assertTrue(parent.receivedEcho(1, ELECTION_ID),
                "with its only child now answered, the node must report the result to its parent; "
                        + "recorded: " + parent.calls());
    }

    @Test
    // The once-only latch is what stops a duplicate answer echoing a second time.
    void aRepeatedEchoFromTheSameChildIsNotPropagatedTwice() throws Exception {
        receiveElectionFromParent();

        worker.receiveEchoMessage(ELECTION_ID, 3, 3, 0);
        worker.receiveEchoMessage(ELECTION_ID, 3, 3, 0); // same child answering again

        assertEquals(1L, countOf(parent.calls(), "echo|"),
                "a node must report upward exactly once; a duplicate echo must not trigger a second propagation; "
                        + "recorded: " + parent.calls());
    }

    @Test
    // Termination means every child replied, so one answer out of two is not enough.
    void waitsForAllChildrenBeforeEchoingToParent() throws Exception {
        otherChild = new RecordingWorkerRemote(4);
        registry.rebind("Worker4", otherChild);
        worker.connectAsNeighbour(4, HOST, TEST_RMI_PORT);

        receiveElectionFromParent();

        // Only the first of two children has answered so far.
        worker.receiveEchoMessage(ELECTION_ID, 3, 3, 0);

        assertEquals(0L, countOf(parent.calls(), "echo|"),
                "with a child still owing an echo this node must not report upward yet - a premature echo tells the "
                        + "parent this subtree is finished, so it can decide a winner without ever hearing from the "
                        + "other child, and the highest id in that subtree is lost; recorded: " + parent.calls());

        // The last child answers, so the subtree is complete.
        worker.receiveEchoMessage(ELECTION_ID, 4, 4, 0);

        assertEquals(1L, countOf(parent.calls(), "echo|"),
                "once the last child has answered, the running best candidate must go up exactly once; recorded: "
                        + parent.calls());
        assertTrue(parent.receivedEcho(1, ELECTION_ID),
                "the echo has to name this node and this election, or the parent cannot match it to the election it "
                        + "is waiting on; recorded: " + parent.calls());
    }

    @Test
    // The initiator has no parent, so completing its wave is what declares the winner.
    void theInitiatorAnnouncesTheWinnerOnceEveryChildHasEchoed() throws Exception {
        // A second, unattached worker: only the child's echo can complete this wave.
        WorkerRemoteImpl initiator = new WorkerRemoteImpl(1);
        try {
            initiator.connectAsNeighbour(3, HOST, TEST_RMI_PORT);

            long electionId = initiator.startElection();
            initiator.receiveEchoMessage(electionId, 3, 3, 0);

            // Both candidates sit on JAC 0, so the tie-break decides it: higher id wins.
            assertEquals(3, initiator.getCurrentCoordinatorId(),
                    "both candidates have JAC 0, so the higher worker id must win the tie");

            assertTrue(receivedCoordinatorFor(child.calls(), 3),
                    "the initiator has no parent, so it must announce the winner to its neighbours; "
                            + "recorded: " + child.calls());
        } finally {
            unexportQuietly(initiator);
        }
    }
}
