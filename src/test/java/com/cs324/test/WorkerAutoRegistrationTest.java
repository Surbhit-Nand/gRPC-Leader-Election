package com.cs324.test;

import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.common.WorkerRegistration;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Auto-registration: monotonic ids, never reused, peer handoff in one atomic call.
class WorkerAutoRegistrationTest {

    private static final int TEST_RMI_PORT = 19888;

    private static Registry registry;

    private BootstrapRegistryImpl bootstrap;

    @BeforeAll
    static void startRegistry() throws Exception {
        registry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @BeforeEach
    void setUp() throws Exception {
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
            // already unexported
        }
    }

    @Test
    void firstAutoRegistrationGetsIdOneAndNullPeer() throws Exception {
        WorkerRegistration reg = bootstrap.registerWorkerAuto("localhost", TEST_RMI_PORT);

        assertEquals(1, reg.workerId());
        assertNull(reg.peerStub());
        assertEquals(List.of(1), bootstrap.getActiveWorkerIds());
        assertEquals(List.of(1), bootstrap.getAllocatedWorkerIds());
    }

    @Test
    void secondAutoRegistrationGetsIdTwoAndFirstWorkerAsPeer() throws Exception {
        WorkerRegistration first = bootstrap.registerWorkerAuto("localhost", TEST_RMI_PORT);
        FakeWorkerRemote worker1 = new FakeWorkerRemote(first.workerId());
        registry.rebind("Worker" + first.workerId(), worker1);
        try {
            WorkerRegistration second = bootstrap.registerWorkerAuto("localhost", TEST_RMI_PORT);

            assertEquals(2, second.workerId());
            assertNotNull(second.peerStub());
            assertEquals(1, second.peerStub().getId());
        } finally {
            unexportQuietly(worker1);
        }
    }

    @Test
    void middleIdIsNotReusedAfterDeregister() throws Exception {
        // Distinct unreachable ports: peer lookup fails with RemoteException (no prune),
        // so all three entries survive unlike same-port lookups that prune on NotBoundException.
        bootstrap.registerWorkerAuto("localhost", 19881);
        bootstrap.registerWorkerAuto("localhost", 19882);
        bootstrap.registerWorkerAuto("localhost", 19883);

        bootstrap.deregisterWorker(2);
        WorkerRegistration next = bootstrap.registerWorkerAuto("localhost", 19884);

        assertEquals(4, next.workerId(), "monotonic ids must never fill the freed middle gap");
        List<Integer> active = bootstrap.getActiveWorkerIds();
        assertEquals(3, active.size());
        assertTrue(active.containsAll(List.of(1, 3, 4)));
    }

    @Test
    void manualHighIdPushesAutoCounterPastIt() throws Exception {
        // Distinct unreachable ports: peer lookup fails with RemoteException (no prune),
        // so the manual entry survives to force the counter jump and the later collision.
        bootstrap.registerWorker(10, "localhost", 19881);

        WorkerRegistration next = bootstrap.registerWorkerAuto("localhost", 19882);

        assertEquals(11, next.workerId(), "auto counter must jump past a manual id to avoid collision");

        // The auto-handed 11 is taken, so a manual claim on it must be rejected as a duplicate.
        RemoteException duplicate = assertThrows(RemoteException.class,
                () -> bootstrap.registerWorker(11, "localhost", 19883));
        assertTrue(duplicate.getMessage().contains("Duplicate worker id"));
    }

    @Test
    void deregisterDoesNotRewindAutoCounter() throws Exception {
        bootstrap.registerWorkerAuto("localhost", TEST_RMI_PORT);
        bootstrap.deregisterWorker(1);

        WorkerRegistration next = bootstrap.registerWorkerAuto("localhost", TEST_RMI_PORT);

        assertEquals(2, next.workerId());
    }
}
