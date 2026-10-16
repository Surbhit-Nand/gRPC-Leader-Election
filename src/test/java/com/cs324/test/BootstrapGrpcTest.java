package com.cs324.test;

import com.cs324.bootstrap.BootstrapGrpcService;
import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.registry.proto.AllocateJobIdRequest;
import com.cs324.registry.proto.DeregisterRequest;
import com.cs324.registry.proto.GetActiveWorkersRequest;
import com.cs324.registry.proto.GetWorkerAddressRequest;
import com.cs324.registry.proto.RegisterRequest;
import com.cs324.registry.proto.RegisterResponse;
import com.cs324.registry.proto.RegistryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Bootstrap discovery over gRPC: same directory, same monotonic-id rules,
// second transport. Real gRPC server on an ephemeral port. Peer handoff still
// verifies liveness over RMI (dual-stack), so the peer test binds one fake.
class BootstrapGrpcTest {

    // Fixed RMI port for the peer-liveness check; clear of other test classes.
    private static final int TEST_RMI_PORT = 19555;

    private static Registry rmiRegistry;

    private BootstrapRegistryImpl directory;
    private Server server;
    private ManagedChannel channel;
    private RegistryServiceGrpc.RegistryServiceBlockingStub registry;

    @BeforeAll
    static void startRmiRegistry() throws Exception {
        rmiRegistry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRmiRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(rmiRegistry, true);
    }

    @BeforeEach
    void setUp() throws Exception {
        directory = new BootstrapRegistryImpl();
        server = ServerBuilder.forPort(0)
                .addService(new BootstrapGrpcService(directory))
                .build()
                .start();
        channel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        registry = RegistryServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : rmiRegistry.list()) {
            try {
                rmiRegistry.unbind(name);
            } catch (Exception ignored) {
                // best effort
            }
        }
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
        unexportQuietly(directory);
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

    private RegisterRequest auto(String host, int port) {
        return RegisterRequest.newBuilder()
                .setHost(host).setPort(port).setAutoAssign(true).build();
    }

    @Test
    void firstAutoRegisterGetsIdOneAndNoPeer() {
        RegisterResponse response = registry.register(auto("localhost", 3500));

        assertEquals(1, response.getWorkerId());
        assertFalse(response.getHasPeer());
    }

    @Test
    void secondAutoRegisterGetsIdTwoAndPeerEndpoint() throws Exception {
        registry.register(auto("localhost", TEST_RMI_PORT));
        // Worker 1 is bound for real, so the directory can hand its endpoint over.
        FakeWorkerRemote worker1 = new FakeWorkerRemote(1);
        rmiRegistry.rebind("Worker1", worker1);
        try {
            RegisterResponse second = registry.register(auto("localhost", TEST_RMI_PORT));

            assertEquals(2, second.getWorkerId());
            assertTrue(second.getHasPeer());
            // Endpoint data, not a stub: id plus the first worker's published address.
            assertEquals(1, second.getPeer().getWorkerId());
            assertEquals("localhost", second.getPeer().getHost());
            assertEquals(TEST_RMI_PORT, second.getPeer().getPort());
        } finally {
            unexportQuietly(worker1);
        }
    }

    @Test
    void manualClaimThenDuplicateMapsToAlreadyExists() {
        RegisterResponse first = registry.register(RegisterRequest.newBuilder()
                .setWorkerId(10).setHost("localhost").setPort(3510)
                .setAutoAssign(false).build());
        assertEquals(10, first.getWorkerId());

        // Auto-assign must jump past the manual id, mirroring the RMI rule.
        RegisterResponse next = registry.register(auto("localhost", 3511));
        assertEquals(11, next.getWorkerId());

        StatusRuntimeException duplicate = assertThrows(StatusRuntimeException.class,
                () -> registry.register(RegisterRequest.newBuilder()
                        .setWorkerId(11).setHost("localhost").setPort(3512)
                        .setAutoAssign(false).build()));
        assertEquals(Status.Code.ALREADY_EXISTS, duplicate.getStatus().getCode());
    }

    @Test
    void middleIdIsNotReusedAfterDeregister() {
        registry.register(auto("localhost", 3500));
        registry.register(auto("localhost", 3501));
        registry.register(auto("localhost", 3502));
        registry.deregister(DeregisterRequest.newBuilder().setWorkerId(2).build());

        RegisterResponse next = registry.register(auto("localhost", 3503));

        assertEquals(4, next.getWorkerId(), "monotonic ids must never fill the freed gap");
        assertTrue(registry.getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance())
                .getWorkerIdsList().containsAll(List.of(1, 3, 4)));
    }

    @Test
    void allocateJobIdStartsAtOneAndIncrements() {
        assertEquals(1, registry.allocateJobId(AllocateJobIdRequest.getDefaultInstance()).getJobId());
        assertEquals(2, registry.allocateJobId(AllocateJobIdRequest.getDefaultInstance()).getJobId());
    }

    @Test
    void workerAddressReportsFoundAndMissing() {
        registry.register(auto("localhost", 3500));

        var present = registry.getWorkerAddress(
                GetWorkerAddressRequest.newBuilder().setWorkerId(1).build());
        assertTrue(present.getFound());
        assertEquals(3500, present.getWorker().getPort());

        var missing = registry.getWorkerAddress(
                GetWorkerAddressRequest.newBuilder().setWorkerId(99).build());
        assertFalse(missing.getFound());
    }

    @Test
    void deregisteringUnknownIdIsQuiet() {
        // Map.remove on a missing key is a no-op, matching the RMI behavior.
        registry.deregister(DeregisterRequest.newBuilder().setWorkerId(42).build());
        assertTrue(registry.getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance())
                .getWorkerIdsList().isEmpty());
    }
}
