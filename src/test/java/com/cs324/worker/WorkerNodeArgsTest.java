package com.cs324.worker;

import com.cs324.common.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Arg parsing + free-port scan for auto-assigned workers. No Bootstrap RMI needed here.
class WorkerNodeArgsTest {

    private final List<Registry> created = new ArrayList<>();

    @AfterEach
    void releaseRegistries() throws Exception {
        for (Registry r : created) {
            try {
                UnicastRemoteObject.unexportObject(r, true);
            } catch (Exception ignored) {
                // best effort
            }
        }
        created.clear();
    }

    @Test
    void noArgsMeansAutoWithDefaults() {
        WorkerNode.WorkerStartupConfig config = WorkerNode.parseArgs(new String[0]);

        assertTrue(config.auto());
        assertEquals("localhost", config.bootstrapHost());
        assertEquals(Constants.DEFAULT_RMI_PORT, config.bootstrapPort());
    }

    @Test
    void twoArgsMeansAutoWithCustomBootstrap() {
        WorkerNode.WorkerStartupConfig config = WorkerNode.parseArgs(new String[]{"myhost", "2099"});

        assertTrue(config.auto());
        assertEquals("myhost", config.bootstrapHost());
        assertEquals(2099, config.bootstrapPort());
    }

    @Test
    void fourArgsMeansHiddenManualOverride() {
        WorkerNode.WorkerStartupConfig config =
                WorkerNode.parseArgs(new String[]{"7", "3600", "myhost", "2099"});

        assertTrue(!config.auto());
        assertEquals(7, config.workerId());
        assertEquals(3600, config.rmiPort());
        assertEquals("myhost", config.bootstrapHost());
        assertEquals(2099, config.bootstrapPort());
    }

    @Test
    void wrongArgCountIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> WorkerNode.parseArgs(new String[]{"only-one"}));
        assertThrows(IllegalArgumentException.class,
                () -> WorkerNode.parseArgs(new String[]{"1", "2", "3"}));
    }

    @Test
    void nonIntegerPortsAreRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkerNode.parseArgs(new String[]{"myhost", "not-a-port"}));
        assertThrows(IllegalArgumentException.class,
                () -> WorkerNode.parseArgs(new String[]{"x", "3500", "h", "p"}));
    }

    @Test
    void grpcFlagSwitchesTransportWithDefaultGrpcPort() {
        WorkerNode.WorkerStartupConfig config =
                WorkerNode.parseArgs(new String[]{"--grpc", "myhost", "2099"});

        assertTrue(config.auto());
        assertTrue(config.grpc());
        assertEquals("myhost", config.bootstrapHost());
        assertEquals(2099, config.bootstrapPort());
        assertEquals(Constants.DEFAULT_GRPC_PORT, config.bootstrapGrpcPort());
    }

    @Test
    void grpcPortFlagImpliesGrpcAndOverridesPort() {
        WorkerNode.WorkerStartupConfig config = WorkerNode.parseArgs(
                new String[]{"--grpc-port", "50060"});

        assertTrue(config.auto());
        assertTrue(config.grpc());
        assertEquals(50060, config.bootstrapGrpcPort());
        assertEquals("localhost", config.bootstrapHost());
    }

    @Test
    void grpcFlagsCombineWithManualOverride() {
        WorkerNode.WorkerStartupConfig config = WorkerNode.parseArgs(
                new String[]{"--grpc", "7", "3600", "myhost", "2099"});

        assertTrue(!config.auto());
        assertTrue(config.grpc());
        assertEquals(7, config.workerId());
        assertEquals(3600, config.rmiPort());
    }

    @Test
    void grpcPortWithoutValueIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkerNode.parseArgs(new String[]{"--grpc-port"}));
    }

    @Test
    void plainFormsUseGrpcRegistration() {
        assertTrue(WorkerNode.parseArgs(new String[0]).grpc());
        assertTrue(WorkerNode.parseArgs(new String[]{"myhost", "2099"}).grpc());
    }

    @Test
    void deployedWorkerDoesNotExportAnRmiObject() throws Exception {
        WorkerRemoteImpl worker = new WorkerRemoteImpl(1, false);
        try {
            assertThrows(java.rmi.NoSuchObjectException.class,
                    () -> java.rmi.server.RemoteObject.toStub(worker));
        } finally { worker.shutdown(); }
    }
}
