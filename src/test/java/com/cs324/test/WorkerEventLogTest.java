package com.cs324.test;

import com.cs324.common.WorkerEvent;
import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Worker-side happening log: every election, edge change, dispatch and
// farewell is recorded with a sequence number for the GUI feed to drain.
class WorkerEventLogTest {

    private static final int PORT = 19666;

    private static Registry registry;

    @BeforeAll
    static void startRegistry() throws Exception {
        registry = LocateRegistry.createRegistry(PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            try {
                registry.unbind(name);
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    private void unexportQuietly(Remote remote) {
        try {
            UnicastRemoteObject.unexportObject(remote, true);
        } catch (NoSuchObjectException ignored) {
            // already gone
        } catch (Exception ignored) {
            // best effort
        }
    }

    private List<String> kindsOf(WorkerRemoteImpl worker) throws Exception {
        return worker.getRecentEvents(-1).stream().map(WorkerEvent::kind).toList();
    }

    @Test
    void electionStartAndAnnouncementAreLogged() throws Exception {
        WorkerRemoteImpl w1 = new WorkerRemoteImpl(1);
        try {
            w1.startElection();

            List<String> kinds = kindsOf(w1);
            assertTrue(kinds.contains(WorkerEvent.ELECTION_STARTED),
                    "every originated wave must be logged, got: " + kinds);
            assertTrue(kinds.contains(WorkerEvent.COORDINATOR_ANNOUNCED),
                    "the lone worker concludes its own wave, got: " + kinds);
        } finally {
            w1.shutdown();
            unexportQuietly(w1);
        }
    }

    @Test
    void edgeChangesAndFarewellAreLogged() throws Exception {
        WorkerRemoteImpl w1 = new WorkerRemoteImpl(1);
        WorkerRemoteImpl w2 = new WorkerRemoteImpl(2);
        registry.rebind("Worker1", w1);
        registry.rebind("Worker2", w2);
        try {
            w1.connectAsNeighbour(2, "localhost", PORT);
            w2.connectAsNeighbour(1, "localhost", PORT);
            assertTrue(kindsOf(w1).contains(WorkerEvent.NEIGHBOUR_ADDED));

            // Graceful exit hands over before the edge is dropped.
            w1.notifyNeighboursOfLeaving();
            assertTrue(kindsOf(w1).contains(WorkerEvent.LEAVING),
                    "the leaver must log its farewell, got: " + kindsOf(w1));
            List<WorkerEvent> survivorLog = w2.getRecentEvents(-1);
            assertTrue(survivorLog.stream().anyMatch(e ->
                            e.kind().equals(WorkerEvent.NEIGHBOUR_REMOVED)
                                    && e.detail().contains("leave")),
                    "the survivor must log the graceful drop, got: " + survivorLog);
        } finally {
            w1.shutdown();
            w2.shutdown();
            unexportQuietly(w1);
            unexportQuietly(w2);
        }
    }

    @Test
    void crashedNeighbourDropIsLogged() throws Exception {
        WorkerRemoteImpl w1 = new WorkerRemoteImpl(1);
        WorkerRemoteImpl w2 = new WorkerRemoteImpl(2);
        registry.rebind("Worker1", w1);
        registry.rebind("Worker2", w2);
        try {
            w1.connectAsNeighbour(2, "localhost", PORT);

            // Crash without farewell: the next wave past the corpse logs the drop.
            registry.unbind("Worker2");
            unexportQuietly(w2);
            w1.startElection();

            List<WorkerEvent> log = w1.getRecentEvents(-1);
            assertTrue(log.stream().anyMatch(e ->
                            e.kind().equals(WorkerEvent.NEIGHBOUR_REMOVED)
                                    && e.detail().contains("unreachable")),
                    "expected an unreachable drop, got: " + log);
            assertTrue(w1.getNeighbourIds().isEmpty());
        } finally {
            w1.shutdown();
            unexportQuietly(w1);
            try {
                w2.shutdown();
            } catch (Exception ignored) {
                // pools still need stopping after the stub is gone
            }
        }
    }

    @Test
    void taskReceiptIsLogged() throws Exception {
        WorkerRemoteImpl w1 = new WorkerRemoteImpl(1);
        try {
            w1.assignTask(41, "MAX", new int[]{3, 7});

            assertTrue(kindsOf(w1).contains(WorkerEvent.TASK_RECEIVED));
        } finally {
            w1.shutdown();
            unexportQuietly(w1);
        }
    }

    @Test
    void adoptCoordinatorIsLogged() throws Exception {
        WorkerRemoteImpl w1 = new WorkerRemoteImpl(1);
        try {
            w1.adoptCoordinator(2, 0x5AD0L);

            assertTrue(kindsOf(w1).contains(WorkerEvent.COORDINATOR_ADOPTED));
        } finally {
            w1.shutdown();
            unexportQuietly(w1);
        }
    }

    @Test
    void logIsSequencedFilteredAndBounded() throws Exception {
        WorkerRemoteImpl w1 = new WorkerRemoteImpl(1);
        try {
            // Each wave logs STARTED + ANNOUNCED: 40 waves overflow the 64-slot ring.
            for (int i = 0; i < 40; i++) {
                w1.startElection();
            }
            List<WorkerEvent> all = w1.getRecentEvents(-1);
            assertEquals(64, all.size(), "the ring must cap, not grow");
            for (int i = 1; i < all.size(); i++) {
                assertTrue(all.get(i).seq() == all.get(i - 1).seq() + 1,
                        "sequences must stay contiguous after trimming");
            }
            long last = all.get(all.size() - 1).seq();
            assertTrue(w1.getRecentEvents(last).isEmpty(), "nothing newer than the cursor");
            assertEquals(1, w1.getRecentEvents(last - 1).size(), "cursor resumes exactly after");
        } finally {
            w1.shutdown();
            unexportQuietly(w1);
        }
    }
}
