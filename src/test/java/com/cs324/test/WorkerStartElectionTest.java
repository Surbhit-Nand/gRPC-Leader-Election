package com.cs324.test;

import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// startElection(): the initiator seeds its own election state with itself as the
// parent, so once every subtree has echoed back it is the node that decides the
// winner and announces the coordinator.
// A lone worker has nothing to wait for and must resolve to itself immediately.
// Most of these need no peers at all; only the fan-out test needs a real registry,
// because 'did the neighbour receive it over RMI?' is the whole point of it.
class WorkerStartElectionTest {

    private static final int TEST_RMI_PORT = 19807;
    private static final String HOST = "localhost";

    // Enough back-to-back elections that a repeated id would show up.
    private static final int RAPID_ELECTIONS = 100;

    private static Registry registry;

    private WorkerRemoteImpl worker;
    private WorkerRemoteImpl otherWorker;
    // Doubles stand in for peers, so what worker 1 actually sent is observable.
    private RecordingWorkerRemote neighbourTwo;   // id 2 - records what worker 1 sends it
    private RecordingWorkerRemote neighbourThree; // id 3 - records what worker 1 sends it

    @BeforeAll
    // Only the fan-out test needs real stubs; the rest run entirely in-process.
    static void startRegistry() throws RemoteException {
        registry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(worker);
        unexportQuietly(otherWorker);
        unexportQuietly(neighbourTwo);
        unexportQuietly(neighbourThree);
    }

    // Each construct exports over RMI, so release them even when a test made none.
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

    // Seeds a JAC by reflection: no public seam exists, and assignTask() does not
    // move the counter. Replace this once the increment has a proper seam.
    private static void seedJobAllocationCounter(WorkerRemoteImpl worker, int value) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("jobAllocationCounter");
        field.setAccessible(true);
        ((AtomicInteger) field.get(worker)).set(value);
    }

    // Reads the audit set: an initiator that skips its own id re-processes cycles.
    private static Set<Long> seenMessageIds(WorkerRemoteImpl worker) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("seenMessageIds");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<Long> ids = (Set<Long>) field.get(worker);
        return ids;
    }

    @SuppressWarnings("unchecked")
    // Reads the live election map, so the seeded wave state can be inspected.
    private static Map<Long, Object> electionsOf(WorkerRemoteImpl worker) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("elections");
        field.setAccessible(true);
        return (Map<Long, Object>) field.get(worker);
    }

    // ElectionState is a private nested class, so the field is found on the value's
    // own runtime class. Inspection only; nothing here arranges state for the worker.
    private static int readElectionInt(Object electionState, String fieldName) throws Exception {
        Field field = electionState.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Integer) field.get(electionState);
    }

    @Test
    // No neighbours means nothing to wait for, so the wave must resolve to self at once.
    void aLoneWorkerWithNoNeighboursElectsItself() throws Exception {
        worker = new WorkerRemoteImpl(7);

        long messageId = worker.startElection();

        assertEquals(7, worker.getCurrentCoordinatorId(),
                "with no neighbours there is nobody to compete with, so the only candidate must win");
        assertTrue(messageId > 0, "startElection should return the id it used for this election");
    }

    @Test
    // Ids come from a counter; a repeat would be dropped as a duplicate elsewhere.
    void everyElectionGetsItsOwnMessageId() throws Exception {
        worker = new WorkerRemoteImpl(7);

        Set<Long> ids = new HashSet<>();
        // Sequential here: the uniqueness claim is per-worker, not across threads.
        for (int i = 0; i < RAPID_ELECTIONS; i++) {
            ids.add(worker.startElection());
        }

        assertEquals(RAPID_ELECTIONS, ids.size(),
                "every election must get a unique id - if two share one, every other worker drops the second as a "
                        + "duplicate and the re-election after a coordinator's term silently never happens");
    }

    @Test
    // Two workers can hold the same wave at once, so their ids must not collide.
    void differentWorkersGenerateDifferentElectionIds() throws Exception {
        worker = new WorkerRemoteImpl(4);
        otherWorker = new WorkerRemoteImpl(5);

        long firstId = worker.startElection();
        long secondId = otherWorker.startElection();

        assertNotEquals(firstId, secondId,
                "two different workers must not pick the same election id - they may be holding the same election at "
                        + "the same time, and one would be discarded as a duplicate of the other, so its subtree "
                        + "never echoes back and the two workers never agree on a coordinator");
    }

    @Test
    // The initiator seeds itself as parent and best candidate, or nobody declares.
    void startElectionInitializesElectionState() throws Exception {
        int initiatorId = 7;
        int startingJac = 3;
        worker = new WorkerRemoteImpl(initiatorId);
        // Non-zero JAC, so a hardcoded zero in the seed would be caught here.
        seedJobAllocationCounter(worker, startingJac);

        long messageId = worker.startElection();

        assertTrue(seenMessageIds(worker).contains(messageId),
                "the initiator has processed its own election, so it must record the id as seen - otherwise the "
                        + "election echoed back around a cycle would look new to it and be processed a second time");

        Object election = electionsOf(worker).get(messageId);
        assertNotNull(election, "startElection must register an election under the id it returns; known ids: "
                + electionsOf(worker).keySet());

        assertEquals(initiatorId, readElectionInt(election, "parentId"),
                "the initiator is its own parent - a state pointing anywhere else makes the echo phase send its "
                        + "result to a neighbour that never asked for it, and the coordinator is never announced");
        assertEquals(initiatorId, readElectionInt(election, "bestCandidateId"),
                "the initiator is the first candidate it has seen, so it starts as its own best candidate");
        assertEquals(startingJac, readElectionInt(election, "bestCandidateJac"),
                "the election must start from the worker's actual JAC ("
                        + startingJac + ") - defaulting to 0 would give a busy worker a free pass to win every "
                        + "election it starts, and the whole JAC comparison in the echo phase would be decided "
                        + "against a number the worker never had");
    }

    @Test
    // Fan-out over real RMI stubs: did each neighbour actually receive the wave?
    void startElectionSendsElectionToEveryNeighbour() throws Exception {
        worker = new WorkerRemoteImpl(1);
        neighbourTwo = new RecordingWorkerRemote(2);
        neighbourThree = new RecordingWorkerRemote(3);

        // Rebinding gives each double a registry name, which is how stubs are found.
        registry.rebind("Worker2", neighbourTwo);
        registry.rebind("Worker3", neighbourThree);

        worker.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
        worker.connectAsNeighbour(3, HOST, TEST_RMI_PORT);

        long messageId = worker.startElection();

        assertTrue(neighbourTwo.receivedElection(1, messageId),
                "an initiator must tell every neighbour it is standing for election - if worker 2 is skipped it "
                        + "never learns about the election, never echoes back, and worker 1 waits on it forever, so "
                        + "no coordinator is ever decided; recorded: " + neighbourTwo.calls());
        assertTrue(neighbourThree.receivedElection(1, messageId),
                "the ELECTION must carry the id startElection returned, so the echo that comes back can be matched "
                        + "to this election; recorded: " + neighbourThree.calls());
    }
}
