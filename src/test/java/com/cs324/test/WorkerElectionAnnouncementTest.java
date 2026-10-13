package com.cs324.test;

import com.cs324.common.WorkerRemote;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Loop-back of a coordinator announcement into the worker that broadcast it.
// In a two-node ring the initiator's own broadcast returns to it; recording the id
// first is what makes that a repeat to drop. Treating it as new would reset the
// winner's term budget and flood the ring a second time.
class WorkerElectionAnnouncementTest {

    private static final int TEST_RMI_PORT = 19809;
    private static final String HOST = "localhost";

    private static Registry registry;

    // The winning initiator; its only neighbour is the double, making a two-node ring.
    private WorkerRemoteImpl w1;
    // Completes the wave and relays announcements back, so the loop-back is real.
    private EchoingWorkerRemote peer;

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
        w1 = new WorkerRemoteImpl(1);
        registry.rebind("Worker1", w1);

        // The double answers elections and relays announcements, so the wave
        // finishes and the announcement travels. A worse JAC makes worker 1 win, which is
        // the case where re-processing the loop-back would reset a budget.
        peer = new EchoingWorkerRemote(2);
        // JAC 5 against worker 1's zero, so the initiator wins and risks a budget reset.
        peer.setJobAllocationCounter(5);
        registry.rebind("Worker2", peer);
        peer.relayAnnouncementsTo((WorkerRemote) registry.lookup("Worker1"));

        // Worker 1's only neighbour is the double: a two-node ring.
        w1.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            try {
                registry.unbind(name);
            } catch (Exception ignored) {
            }
        }
        unexportQuietly(w1);
        unexportQuietly(peer);
    }

    // Constructing a worker exports it, so each test releases its own RMI endpoint.
    private void unexportQuietly(Remote remoteObject) {
        if (remoteObject == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
        }
    }

    // Counted on the double, which is the only node that can witness a relay.
    private long countCalls(String prefix) {
        return peer.calls().stream().filter(call -> call.startsWith(prefix)).count();
    }

    @Test
    // The winner records its own announcement id before broadcasting, so the loop-back
    // is a repeat it can drop rather than a new term that would restart its budget.
    void aWinningInitiatorDoesNotReprocessItsOwnAnnouncement() throws Exception {
        w1.startElection();

        assertEquals(1L, countCalls("election|"),
                "the wave should have reached the only neighbour exactly once");
        assertEquals(1, w1.getCurrentCoordinatorId(),
                "worker 1 has the lowest JAC, so it wins and leads the new term");
        assertEquals(1L, countCalls("coordinator|"),
                "the winner's announcement must reach the ring once. Meeting its own broadcast "
                        + "coming back is a repeat, not a new term, so it must not relay it again; "
                        + "recorded on worker 2: " + peer.calls());
        assertTrue(peer.receivedCoordinator(1, announcementIdFrom(peer)),
                "the announcement should name the worker that won the election; recorded on worker 2: "
                        + peer.calls());
    }

    // The single coordinator announcement id the double recorded.
    private long announcementIdFrom(EchoingWorkerRemote recorder) {
        for (String call : recorder.calls()) {
            if (call.startsWith("coordinator|")) {
                return Long.parseLong(call.substring(call.lastIndexOf('|') + 1));
            }
        }
        throw new AssertionError("no coordinator announcement was recorded on worker 2: " + recorder.calls());
    }
}
