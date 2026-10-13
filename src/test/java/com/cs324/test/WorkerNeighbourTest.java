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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Step 1 of the worker test plan: connectAsNeighbour().
// The network only forms if connecting both resolves the peer through RMI and records it.
// getNeighbourIds() exposes the recorded set, so recording is assertable directly too.
// Once connected, a relayed message has to actually arrive at the neighbour.
class WorkerNeighbourTest {

    private static final int TEST_RMI_PORT = 19801;
    private static final String HOST = "localhost";

    // Shared across tests; recreating an RMI registry on one port between tests is flaky.
    private static Registry registry;

    // A real worker, so the production connect path is what is under test.
    private WorkerRemoteImpl worker;
    // Created per test by the tests that need one; tearDown tolerates it staying null.
    private RecordingWorkerRemote neighbour;

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
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(worker);
        unexportQuietly(neighbour);
        neighbour = null;
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

    @Test
    void connectingToAWorkerThatIsBoundSucceeds() throws Exception {
        neighbour = new RecordingWorkerRemote(2);
        registry.rebind("Worker2", neighbour);

        assertDoesNotThrow(() -> worker.connectAsNeighbour(2, HOST, TEST_RMI_PORT),
                "connecting to an active worker should succeed - the successful RMI call is the acknowledgement");
    }

    @Test
    void connectingToAWorkerThatIsNotBoundFailsLoudly() throws Exception {
        // Worker 9 is not running and is bound in no registry.
        assertThrows(RemoteException.class,
                () -> worker.connectAsNeighbour(9, HOST, TEST_RMI_PORT),
                "an unreachable neighbour must fail loudly instead of silently leaving the network half-formed");
    }

    @Test
    void aConnectedNeighbourIsRememberedAndReceivesPropagatedMessages() throws Exception {
        neighbour = new RecordingWorkerRemote(2);
        registry.rebind("Worker2", neighbour);

        worker.connectAsNeighbour(2, HOST, TEST_RMI_PORT);

        // Announcement relaying reaches every recorded neighbour, which is the proof.
        // A fixed message id keeps the assertion exact without involving an election.
        worker.receiveCoordinatorMessage(2, 500L);

        assertTrue(neighbour.receivedCoordinator(2, 500L),
                "after connecting, the neighbour must receive relayed messages; recorded: " + neighbour.calls());
    }
}
