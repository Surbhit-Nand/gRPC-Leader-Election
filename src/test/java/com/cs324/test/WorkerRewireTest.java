package com.cs324.test;

import com.cs324.bootstrap.BootstrapRegistryImpl;
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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Partition healing: a middle worker's neighbours must chain to the grandparent,
// never split into islands. Uses real RMI between live WorkerRemoteImpl instances.
class WorkerRewireTest {

    private static final int PORT = 19777;

    private static Registry registry;

    private BootstrapRegistryImpl bootstrap;

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
        for (String name : safeList()) {
            try {
                registry.unbind(name);
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    private String[] safeList() {
        try {
            return registry.list();
        } catch (Exception e) {
            return new String[0];
        }
    }

    private void unexportQuietly(Remote r) {
        try {
            UnicastRemoteObject.unexportObject(r, true);
        } catch (NoSuchObjectException ignored) {
            // already gone
        } catch (Exception ignored) {
            // best effort
        }
    }

    // Wires A-B-C as a chain over one shared test registry (all addresses are localhost:PORT).
    private WorkerRemoteImpl[] chainOfThree() throws Exception {
        bootstrap = new BootstrapRegistryImpl();
        WorkerRemoteImpl w1 = new WorkerRemoteImpl(1);
        WorkerRemoteImpl w2 = new WorkerRemoteImpl(2);
        WorkerRemoteImpl w3 = new WorkerRemoteImpl(3);
        registry.rebind("Worker1", w1);
        registry.rebind("Worker2", w2);
        registry.rebind("Worker3", w3);
        bootstrap.registerWorker(1, "localhost", PORT);
        bootstrap.registerWorker(2, "localhost", PORT);
        bootstrap.registerWorker(3, "localhost", PORT);

        // B-C first, then A-B, then re-seed C so every 2-hop cache knows the grandparent.
        w2.connectAsNeighbour(3, "localhost", PORT);
        w3.connectAsNeighbour(2, "localhost", PORT);
        w1.connectAsNeighbour(2, "localhost", PORT);
        w2.connectAsNeighbour(1, "localhost", PORT);
        w3.connectAsNeighbour(2, "localhost", PORT);

        w1.setBootstrapReference(bootstrap, "localhost");
        w2.setBootstrapReference(bootstrap, "localhost");
        w3.setBootstrapReference(bootstrap, "localhost");
        return new WorkerRemoteImpl[]{w1, w2, w3};
    }

    private void shutdownAll(WorkerRemoteImpl... workers) {
        for (WorkerRemoteImpl w : workers) {
            try {
                w.shutdown();
            } catch (Exception ignored) {
                // best effort; already-unexported workers still shut their pools down cleanly
            }
            unexportQuietly(w);
        }
        if (bootstrap != null) {
            unexportQuietly(bootstrap);
            bootstrap = null;
        }
    }

    @Test
    void crashOfMiddleWorkerChainsOrphansToGrandparent() throws Exception {
        WorkerRemoteImpl[] chain = chainOfThree();
        WorkerRemoteImpl w1 = chain[0];
        WorkerRemoteImpl w2 = chain[1];
        WorkerRemoteImpl w3 = chain[2];
        try {
            // Simulate a crash: B disappears without any farewell.
            registry.unbind("Worker2");
            unexportQuietly(w2);
            bootstrap.deregisterWorker(2);

            // Any traffic past the corpse triggers detection + rewire.
            w1.startElection();

            assertTrue(w1.getNeighbourIds().contains(3),
                    "A must chain to grandparent C after B crashes, or the cluster splits");
            assertTrue(w3.getNeighbourIds().contains(1),
                    "the repair edge must be bidirectional");
            assertFalse(w1.getNeighbourIds().contains(2), "the dead edge must be pruned");
        } finally {
            // w2's pools still need shutting down even though its stub is already gone.
            shutdownAll(w1, w2, w3);
        }
    }

    @Test
    void gracefulLeaveHandsGrandparentToSurvivors() throws Exception {
        WorkerRemoteImpl[] chain = chainOfThree();
        WorkerRemoteImpl w1 = chain[0];
        WorkerRemoteImpl w3 = chain[2];
        try {
            // Clean exit: B hands its neighbour set over before deregistering.
            Set<Integer> bNeighbours = Set.of(1, 3);
            w1.notifyLeaving(2, bNeighbours);
            w3.notifyLeaving(2, bNeighbours);

            assertTrue(w1.getNeighbourIds().contains(3), "A must learn C from B's farewell");
            assertTrue(w3.getNeighbourIds().contains(1), "C must learn A from B's farewell");
        } finally {
            shutdownAll(chain[0], chain[1], chain[2]);
        }
    }

    @Test
    void unknownGrandparentFallsBackToBootstrapPeer() throws Exception {
        bootstrap = new BootstrapRegistryImpl();
        WorkerRemoteImpl w1 = new WorkerRemoteImpl(1);
        WorkerRemoteImpl w4 = new WorkerRemoteImpl(4);
        registry.rebind("Worker1", w1);
        registry.rebind("Worker4", w4);
        bootstrap.registerWorker(1, "localhost", PORT);
        bootstrap.registerWorker(4, "localhost", PORT);
        w1.setBootstrapReference(bootstrap, "localhost");
        w4.setBootstrapReference(bootstrap, "localhost");
        try {
            // Farewell names only the dead and us: no grandparent is known.
            w1.notifyLeaving(2, Set.of(1, 2));

            assertTrue(w1.getNeighbourIds().contains(4),
                    "with no grandparent known, the orphan must take any live Bootstrap peer");
        } finally {
            shutdownAll(w1, w4);
        }
    }
}
