package com.cs324.worker;

import com.cs324.common.Constants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Arg parsing for the gRPC-only worker CLI.
class WorkerNodeArgsTest {

    @Test
    void noArgsMeansAutoWithDefaults() {
        WorkerNode.WorkerStartupConfig config = WorkerNode.parseArgs(new String[0]);

        assertEquals("localhost", config.bootstrapHost());
        assertEquals(Constants.DEFAULT_GRPC_PORT, config.bootstrapGrpcPort());
    }

    @Test
    void twoArgsSelectBootstrapHostAndGrpcPort() {
        WorkerNode.WorkerStartupConfig config = WorkerNode.parseArgs(new String[]{"myhost", "2099"});

        assertEquals("myhost", config.bootstrapHost());
        assertEquals(2099, config.bootstrapGrpcPort());
    }

    @Test
    void grpcPortFlagOverridesDefault() {
        WorkerNode.WorkerStartupConfig config = WorkerNode.parseArgs(
                new String[]{"--grpc-port", "50060"});

        assertEquals("localhost", config.bootstrapHost());
        assertEquals(50060, config.bootstrapGrpcPort());
    }

    @Test
    void grpcPortFlagOverridesPositionalPort() {
        WorkerNode.WorkerStartupConfig config = WorkerNode.parseArgs(
                new String[]{"--grpc-port", "50060", "myhost", "2099"});

        assertEquals("myhost", config.bootstrapHost());
        assertEquals(50060, config.bootstrapGrpcPort());
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
                () -> WorkerNode.parseArgs(new String[]{"myhost", "70000"}));
    }

    @Test
    void grpcPortWithoutValueIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkerNode.parseArgs(new String[]{"--grpc-port"}));
    }
}
