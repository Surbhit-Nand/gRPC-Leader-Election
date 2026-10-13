package com.cs324.test;

import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.common.WorkerRemote;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Covers the join handoff a new worker goes through to enter an existing network.
// It drives the same calls WorkerNode makes, so worker startup has a fixed contract.
// Registering, receiving a live peer stub, and connecting to that peer are each exercised.
// Own port, separate from BootstrapRegistryImplTest, so the classes never share a registry.
class WorkerJoinTest {

    private static final int TEST_RMI_PORT = 19998;
    private static final String HOST = "localhost";

    // Shared across tests; recreating a registry on one port between tests is flaky.
    private static Registry registry;

    private BootstrapRegistryImpl bootstrap;

    @BeforeAll
    static void startRegistry() throws RemoteException {
        registry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @BeforeEach
    void setUp() throws RemoteException {
        bootstrap = new BootstrapRegistryImpl();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(bootstrap);
    }

    private void unexportQuietly(Remote remoteObject) {
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
            // already unexported, nothing to do
        }
    }

    @Test
    void aJoiningWorkerCanConnectToThePeerTheBootstrapHandedIt() throws Exception {
        // Worker 1 is registered and bound, so the directory has a live stub to introduce.
        FakeWorkerRemote worker1 = new FakeWorkerRemote(1);
        registry.rebind("Worker1", worker1);
        bootstrap.registerWorker(1, HOST, TEST_RMI_PORT);

        // Worker 2 joins and is handed an existing active worker.
        WorkerRemote peer = bootstrap.registerWorker(2, HOST, TEST_RMI_PORT);
        assertNotNull(peer, "a joining worker must be handed a peer, otherwise the network can never form");

        // This is the call WorkerNode makes, so a wrong binding name fails right here.
        assertDoesNotThrow(() -> peer.connectAsNeighbour(2, HOST, TEST_RMI_PORT),
                "the peer stub handed out by the Bootstrap must accept the neighbour connection over RMI");

        unexportQuietly(worker1);
    }

    @Test
    void registeringWhileTheChosenPeerIsNotBoundSkipsStalePeerAndKeepsNewWorker() throws Exception {
        // Worker 1 is listed but never bound, as if it had crashed or renamed itself.
        bootstrap.registerWorker(1, HOST, TEST_RMI_PORT);

        // The stale peer is skipped and pruned, and worker 2 still joins with no peer.
        WorkerRemote peer = assertDoesNotThrow(() -> bootstrap.registerWorker(2, HOST, TEST_RMI_PORT),
                "a stale peer must be skipped, not fail the whole join");
        assertNull(peer, "with only a stale peer available there is no live peer to hand out");
        // The newcomer must survive a failed peer lookup and stay in the active list.
        assertTrue(bootstrap.getActiveWorkerIds().contains(2),
                "fixed behaviour: the failed peer lookup must not drop worker 2 from the active list");
    }
}
