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
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// One hop of receiveElectionMessage, over real RMI with recording doubles on both sides.
// Two rules pull against each other: an ELECTION must reach every neighbour except the one it
// arrived from, and an already-processed id must not be re-forwarded or a cycle loops forever.
// But the second rule cannot mean dropping it. Whoever forwarded to us is waiting for a reply,
// so a duplicate must still be answered - and it can arrive from a different neighbour than the
// first copy, which is exactly the case where rule 1 alone would forward the election back.
class WorkerElectionMessageTest {

    private static final int TEST_RMI_PORT = 19802;
    private static final String HOST = "localhost";
    private static final long ELECTION_ID = 42L;

    private static Registry registry;

    // worker is real; the two neighbours are recording doubles, so arrival is assertable.
    private WorkerRemoteImpl worker;      // id 1 - the node under test
    private RecordingWorkerRemote sender; // id 2 - the neighbour the ELECTION arrives from
    private RecordingWorkerRemote other;  // id 3 - the neighbour it must be forwarded to

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
        // Two neighbours only, and the node under test is never its own neighbour.
        worker = new WorkerRemoteImpl(1);
        sender = new RecordingWorkerRemote(2);
        other = new RecordingWorkerRemote(3);

        registry.rebind("Worker2", sender);
        registry.rebind("Worker3", other);

        worker.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
        worker.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Every binding must go, or the next test resolves a double left over from this one.
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(worker);
        unexportQuietly(sender);
        unexportQuietly(other);
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

    // Prefix-matched, so a call kind can be counted without parsing the rest of the entry.
    private static long countOf(List<String> calls, String prefix) {
        return calls.stream().filter(call -> call.startsWith(prefix)).count();
    }

    @Test
    void anElectionIsForwardedToEveryNeighbourExceptTheSender() throws Exception {
        // Arrived from worker 2, so the wave may only leave through worker 3.
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID);

        assertTrue(other.receivedElection(1, ELECTION_ID),
                "the election must be passed on to the other neighbours; recorded: " + other.calls());

        // Back to worker 2 would bounce it off its own parent; worker 3 must get it.
        assertFalse(sender.receivedElection(1, ELECTION_ID),
                "the election must not be sent back to the neighbour it came from; recorded: " + sender.calls());
    }

    @Test
    void aDuplicateElectionIsNotForwardedTwiceButIsStillAnswered() throws Exception {
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID);
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID); // same election id again

        assertEquals(1L, countOf(other.calls(), "election|"),
                "a duplicate must not be forwarded a second time; recorded: " + other.calls());

        // Answering is not optional - the parent is blocked until this echo arrives.
        assertEquals(1L, countOf(sender.calls(), "echo|"),
                "a duplicate must still be answered with exactly one echo - dropping it silently leaves the "
                        + "forwarding node waiting forever, which deadlocks any network containing a cycle; "
                        + "recorded: " + sender.calls());
    }

    @Test
    void duplicateElectionFromDifferentNeighbourIsNotForwardedAgainAndIsEchoedBack() throws Exception {
        // First copy, arriving from worker 2, so it goes on to worker 3.
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID);

        assertEquals(1L, countOf(other.calls(), "election|"),
                "the first copy must be passed on to the neighbours other than the one it came from; recorded: "
                        + other.calls());

        // Same election id, but arriving from the OTHER neighbour: in a network with a cycle
        // this is the election coming back by a different path, and the one case where rule 1
        // alone is wrong - it would now mean forwarding to worker 2, which already sent it.
        worker.receiveElectionMessage(3, 3, 0, ELECTION_ID);

        assertEquals(1L, countOf(other.calls(), "election|"),
                "a duplicate must not start a second propagation, however it reached us; recorded: "
                        + other.calls());
        assertEquals(1L, countOf(other.calls(), "echo|"),
                "the neighbour that forwarded the duplicate is now waiting on our reply, so it must be answered "
                        + "exactly once; recorded: " + other.calls());
        assertTrue(other.receivedEcho(1, ELECTION_ID),
                "the echo has to be attributed to this node and this election, or the duplicate's sender cannot "
                        + "match it to the election it is waiting on; recorded: " + other.calls());
        assertEquals(0L, countOf(sender.calls(), "election|"),
                "the election must not be sent back to worker 2, which is where it came from and where the duplicate "
                        + "would send it next - that just keeps the election circulating around the cycle forever; "
                        + "recorded: " + sender.calls());
    }

    @Test
    void aWorkerWithNoOtherNeighboursEchoesStraightBack() throws Exception {
        // This worker's only neighbour is the one that sent the ELECTION, so it
        // has no children to wait for and must finish immediately.
        WorkerRemoteImpl leaf = new WorkerRemoteImpl(9);
        try {
            leaf.connectAsNeighbour(2, HOST, TEST_RMI_PORT);

            leaf.receiveElectionMessage(2, 2, 0, ELECTION_ID);

            assertEquals(1L, countOf(sender.calls(), "echo|"),
                    "a node with no children has already finished, so it must echo back immediately; "
                            + "recorded: " + sender.calls());
        } finally {
            unexportQuietly(leaf);
        }
    }

    @Test
    void copiesArrivingAtTheSameInstantAreForwardedOnceAndAnsweredOnce() throws Exception {
        // Two copies of one wave released together, so the second lands while the first is still
        // being processed. Claiming the id before publishing the wave used to leave that copy
        // with no state to answer from, and its sender waited for an echo that never came. The
        // window is microseconds wide, so hammer it rather than place a delivery inside it.
        final int attempts = 300;
        // A fresh id per attempt, so one round cannot mask another's duplicates.
        for (int attempt = 0; attempt < attempts; attempt++) {
            final long messageId = 900L + attempt;
            CountDownLatch fire = new CountDownLatch(1);

            Thread fromSender = new Thread(() -> deliverElection(fire, 2, messageId), "copy-from-2");
            Thread fromOther = new Thread(() -> deliverElection(fire, 3, messageId), "copy-from-3");
            fromSender.start();
            fromOther.start();
            fire.countDown();
            fromSender.join(5_000);
            fromOther.join(5_000);

            // Which copy is processed first is not deterministic, so the invariant is counted
            // across both neighbours: the wave goes out once, and the loser is still answered.
            assertEquals(1L, deliveriesFor("election|", messageId),
                    "attempt " + attempt + ": the wave must be forwarded exactly once however many copies "
                            + "arrived; recorded: " + sender.calls() + " / " + other.calls());
            assertEquals(1L, deliveriesFor("echo|", messageId),
                    "attempt " + attempt + ": the copy that lost the race must still be answered, or the "
                            + "neighbour that sent it waits forever; recorded: " + sender.calls() + " / "
                            + other.calls());
        }
    }

    // Delivers one copy of an election, waiting on the gate so copies really do race.
    private void deliverElection(CountDownLatch fire, int fromWorkerId, long messageId) {
        try {
            fire.await();
            worker.receiveElectionMessage(fromWorkerId, fromWorkerId, 0, messageId);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (RemoteException failure) {
            throw new IllegalStateException(failure);
        }
    }

    // Counts one message kind for a single id across both neighbours, since either may be the
    // parent that forwarded and either may be the child still waiting on an answer.
    private long deliveriesFor(String prefix, long messageId) {
        String suffix = "|" + messageId;
        long count = 0;
        for (String call : sender.calls()) {
            if (call.startsWith(prefix) && call.endsWith(suffix)) {
                count++;
            }
        }
        for (String call : other.calls()) {
            if (call.startsWith(prefix) && call.endsWith(suffix)) {
                count++;
            }
        }
        return count;
    }
}
