package com.cs324.bootstrap;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BootstrapRegistryLeaseTest {

    @Test
    void aSilentWorkerIsRemovedAfterItsLeaseExpires() throws Exception {
        BootstrapRegistryImpl registry = new BootstrapRegistryImpl();
        registry.registerWorkerAuto("localhost", 0);
        assertEquals(List.of(1), registry.getActiveWorkerIds());

        Thread.sleep(50);
        assertEquals(List.of(1), registry.expireIdle(25));
        assertTrue(registry.getActiveWorkerIds().isEmpty());
    }

    @Test
    void publishingAnEndpointRefreshesTheLease() throws Exception {
        BootstrapRegistryImpl registry = new BootstrapRegistryImpl();
        registry.registerWorkerAuto("localhost", 0);
        Thread.sleep(50);
        registry.publishGrpcEndpoint(1, "localhost", 0, 12345);

        assertTrue(registry.expireIdle(500).isEmpty());
        assertEquals(List.of(1), registry.getActiveWorkerIds());
    }

    @Test
    void deregistrationRemovesMembershipImmediately() {
        BootstrapRegistryImpl registry = new BootstrapRegistryImpl();
        registry.registerWorkerAuto("localhost", 0);
        registry.deregisterWorker(1);
        assertTrue(registry.getActiveWorkerIds().isEmpty());
    }
}
