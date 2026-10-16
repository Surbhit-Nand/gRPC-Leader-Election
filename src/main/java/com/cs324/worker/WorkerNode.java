package com.cs324.worker;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.Constants;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerRegistration;
import com.cs324.common.WorkerRemote;
import com.cs324.registry.proto.DeregisterRequest;
import com.cs324.registry.proto.RegisterRequest;
import com.cs324.registry.proto.RegisterResponse;
import com.cs324.registry.proto.RegistryServiceGrpc;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

// Entry point for a Worker node: auto-assigns id + port, registers, joins a peer, serves elections and jobs.
public class WorkerNode {

    // Tag for a locally generated adoption id, in a range no election counter reaches.
    private static final long ADOPT_MESSAGE_TAG = 0x5AD0L;

    // Parsed startup request: either fully automatic or a hidden manual override (tests/debug).
    // grpc switches registration/deregistration to the Bootstrap gRPC service;
    // neighbour links, heartbeats and jobs stay on RMI until their owners migrate.
    record WorkerStartupConfig(boolean auto, int workerId, int rmiPort,
            String bootstrapHost, int bootstrapPort, boolean grpc, int bootstrapGrpcPort) {}

    // Holder for a freshly created local registry plus the port it actually bound.
    record LocalRegistry(Registry registry, int port) {}

    public static void main(String[] args) {
        WorkerStartupConfig config;
        try {
            config = parseArgs(args);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.out.println("Usage: WorkerNode [--grpc [--grpc-port N]] [bootstrapHost bootstrapPort]");
            System.out.println("   or: WorkerNode [--grpc [--grpc-port N]] <workerId> <rmiPort> <bootstrapHost> <bootstrapPort> (manual override)");
            System.exit(1);
            return;
        }

        String bootstrapHost = config.bootstrapHost();
        int bootstrapPort = config.bootstrapPort();

        // localhost is right for a single-host demo; a real hostname is needed for multi-host.
        String myHost = "localhost";

        // Assigned here so the shutdown hook below can reach the same instance.
        WorkerRemoteImpl self = null;
        Registry localRegistry = null;
        int workerId = -1;
        int rmiPort = -1;
        // Array holder so the hook can read a bootstrap that may never have been created.
        final BootstrapRemote[] bootstrap = new BootstrapRemote[1];
        // gRPC channel and stub, only set in --grpc mode; the hook closes/deregisters them.
        final ManagedChannel[] grpcChannel = new ManagedChannel[1];
        final RegistryServiceGrpc.RegistryServiceBlockingStub[] grpcRegistry =
                new RegistryServiceGrpc.RegistryServiceBlockingStub[1];
        try {
            Registry bootstrapRegistry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
            bootstrap[0] = (BootstrapRemote) bootstrapRegistry.lookup("BootstrapRegistry");
            if (config.grpc()) {
                grpcChannel[0] = ManagedChannelBuilder
                        .forAddress(bootstrapHost, config.bootstrapGrpcPort())
                        .usePlaintext()
                        .build();
                grpcRegistry[0] = RegistryServiceGrpc.newBlockingStub(grpcChannel[0]);
            }
            if (config.auto()) {
                // Reserve the port first: no id is burned if every port is taken.
                LocalRegistry local = createRegistryOnFreePort(
                        Constants.WORKER_BASE_RMI_PORT, Constants.WORKER_MAX_PORT_TRIES);
                localRegistry = local.registry();
                rmiPort = local.port();

                // One atomic call: id allocation + directory insert + join peer.
                // In --grpc mode the id comes from the RegistryService response;
                // the peer endpoint is resolved back to an RMI stub because
                // neighbour links stay on RMI until their owners migrate.
                WorkerRemote peerStub;
                if (grpcRegistry[0] != null) {
                    GrpcRegistration granted =
                            registerViaGrpc(grpcRegistry[0], -1, myHost, rmiPort);
                    workerId = granted.workerId();
                    peerStub = lookupGrpcPeer(granted);
                } else {
                    WorkerRegistration registration = bootstrap[0].registerWorkerAuto(myHost, rmiPort);
                    workerId = registration.workerId();
                    peerStub = registration.peerStub();
                }
                self = bindLocalWorker(localRegistry, workerId, rmiPort,
                        grpcRegistry[0] != null ? "auto-assigned (grpc)" : "auto-assigned");
                joinViaPeer(self, bootstrap[0], peerStub,
                        workerId, myHost, rmiPort, bootstrapHost, bootstrapPort);
            } else {
                workerId = config.workerId();
                rmiPort = config.rmiPort();
                // Every worker owns its own registry, so each node needs a distinct port.
                localRegistry = LocateRegistry.createRegistry(rmiPort);
                self = bindLocalWorker(localRegistry, workerId, rmiPort,
                        grpcRegistry[0] != null ? "manual (grpc)" : "manual");

                // Registration also returns the one peer to join, or null if we are first.
                WorkerRemote peerStub;
                if (grpcRegistry[0] != null) {
                    GrpcRegistration granted =
                            registerViaGrpc(grpcRegistry[0], workerId, myHost, rmiPort);
                    peerStub = lookupGrpcPeer(granted);
                } else {
                    peerStub = bootstrap[0].registerWorker(workerId, myHost, rmiPort);
                }
                joinViaPeer(self, bootstrap[0], peerStub,
                        workerId, myHost, rmiPort, bootstrapHost, bootstrapPort);
            }
            // Heartbeats start last: a silent coordinator is what triggers a new election.
            self.setBootstrapReference(bootstrap[0], myHost);
            self.enableGrpcJobs(bootstrapHost, config.bootstrapGrpcPort(), myHost, rmiPort);
        } catch (Exception e) {
            System.out.println("Failed to start Worker Node: " + e.getMessage());
            // Best effort: release a registry we reserved but never registered.
            if (self == null && localRegistry != null) {
                try {
                    java.rmi.server.UnicastRemoteObject.unexportObject(localRegistry, true);
                } catch (Exception ignored) {
                    // already released
                }
            }
            System.exit(1);
        }

        final int idForHook = workerId;
        // Final copy so the hook and the startup thread share one instance.
        final WorkerRemoteImpl worker = self;
        if (worker != null) {
            // The cluster elects itself: after a grace period, a worker with no coordinator starts a wave.
            // The delay lets peers adopt the first leader, so startup costs one election, not N.
            Thread startupElection = new Thread(() -> {
                try {
                    Thread.sleep(Constants.STARTUP_ELECTION_DELAY_MS);
                    // Still unknown after the grace period, so this node must start the wave.
                    if (worker.getCurrentCoordinatorId() == -1) {
                        System.out.println("No coordinator known after startup - starting an election.");
                        worker.startElection();
                    } else {
                        System.out.println("Coordinator " + worker.getCurrentCoordinatorId()
                                + " already known - no startup election needed.");
                    }
                } catch (Exception e) {
                    System.out.println("Startup election skipped: " + e.getMessage());
                }
            }, "worker-startup-election-" + idForHook);
            // Daemon: must never hold the JVM open on its own.
            startupElection.setDaemon(true);
            startupElection.start();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            notifyNeighboursOfLeaving(worker, idForHook);
            try {
                // Pool first: leaked pool threads are what stop a JVM from exiting.
                if (worker != null) {
                    worker.shutdown();
                }
            } catch (Exception ignored) {
                // best effort on shutdown
            }
            try {
                // Deregister over whichever transport registered us.
                if (grpcRegistry[0] != null) {
                    grpcRegistry[0].deregister(DeregisterRequest.newBuilder()
                            .setWorkerId(idForHook).build());
                } else if (bootstrap[0] != null) {
                    bootstrap[0].deregisterWorker(idForHook);
                }
            } catch (Exception ignored) {
                // best effort on shutdown
            }
            try {
                if (grpcChannel[0] != null) {
                    grpcChannel[0].shutdown();
                }
            } catch (Exception ignored) {
                // best effort on shutdown
            }
            try {
                if (worker != null) {
                    java.rmi.server.UnicastRemoteObject.unexportObject(worker, true);
                }
            } catch (Exception ignored) {
                // already unexported
            }
        }));

        runMenu(worker, idForHook);
        notifyNeighboursOfLeaving(worker, idForHook);

        // RMI export holds non-daemon threads open, so returning would never end the JVM.
        System.exit(0);
    }

    // Graceful-leave fast path: hand our neighbour set to every neighbour so survivors
    // chain to a grandparent immediately; crashes skip this and rely on failure detection.
    private static void notifyNeighboursOfLeaving(WorkerRemoteImpl self, int workerId) {
        if (self == null) {
            return;
        }
        try {
            int notified = self.notifyNeighboursOfLeaving();
            System.out.println("Handoff sent to " + notified + " neighbour(s) before leaving.");
        } catch (Exception ignored) {
            // best effort on shutdown
        }
    }

    // Assigned id plus the peer endpoint the RegistryService handed back.
    // workerId -1 means auto-assign (registerWorkerAuto semantics on the server).
    record GrpcRegistration(int workerId, WorkerEndpoint peer) {}

    // Registers over gRPC: one atomic allocate/claim + insert + peer handoff.
    // A duplicate manual id surfaces as ALREADY_EXISTS and fails startup below.
    static GrpcRegistration registerViaGrpc(
            RegistryServiceGrpc.RegistryServiceBlockingStub registry,
            int workerId, String myHost, int rmiPort) {
        RegisterRequest.Builder request = RegisterRequest.newBuilder()
                .setHost(myHost)
                .setPort(rmiPort)
                .setAutoAssign(workerId == -1);
        if (workerId != -1) {
            request.setWorkerId(workerId);
        }
        RegisterResponse response = registry.register(request.build());
        System.out.println("Registered over gRPC as Worker " + response.getWorkerId()
                + (response.getHasPeer()
                        ? " (peer Worker " + response.getPeer().getWorkerId() + ")"
                        : " (first worker)"));
        return new GrpcRegistration(response.getWorkerId(),
                response.getHasPeer() ? response.getPeer() : null);
    }

    // Resolves the handed-back endpoint to the RMI stub the neighbour link needs.
    // Null when first, or when the peer died between handoff and lookup.
    static WorkerRemote lookupGrpcPeer(GrpcRegistration granted) {
        if (granted.peer() == null) {
            return null;
        }
        try {
            Registry peerRegistry = LocateRegistry.getRegistry(
                    granted.peer().getHost(), granted.peer().getPort());
            return (WorkerRemote) peerRegistry.lookup("Worker" + granted.peer().getWorkerId());
        } catch (Exception e) {
            System.out.println("Peer endpoint unreachable: " + e.getMessage());
            return null;
        }
    }

    // Creates the impl and binds it locally; peers look this worker up under exactly this name.
    private static WorkerRemoteImpl bindLocalWorker(Registry localRegistry, int workerId, int rmiPort,
            String tag) throws RemoteException {
        WorkerRemoteImpl self = new WorkerRemoteImpl(workerId);
        try {
            self.startProgressServer();
        } catch (java.io.IOException e) {
            self.shutdown();
            java.rmi.server.UnicastRemoteObject.unexportObject(self, true);
            throw new RemoteException("Cannot start gRPC progress server", e);
        }
        localRegistry.rebind("Worker" + workerId, self);
        System.out.println("Worker " + workerId + " bound as Worker" + workerId
                + " on port " + rmiPort + " (" + tag + ")");
        return self;
    }
    // or 4 args (hidden manual override for tests/debug). Anything else is rejected.
    // Leading flags: --grpc registers over gRPC instead of RMI;
    // --grpc-port N overrides the Bootstrap gRPC port (default 50051, implies --grpc).
    static WorkerStartupConfig parseArgs(String[] args) {
        List<String> rest = new ArrayList<>(Arrays.asList(args));
        boolean grpc = rest.remove("--grpc");
        int grpcPort = Constants.DEFAULT_GRPC_PORT;
        int portFlag = rest.indexOf("--grpc-port");
        if (portFlag != -1) {
            if (portFlag + 1 >= rest.size()) {
                throw new IllegalArgumentException("Missing value for --grpc-port.");
            }
            grpcPort = parsePort(rest.remove(portFlag + 1), "grpcPort");
            rest.remove(portFlag);
            grpc = true;
        }
        if (rest.isEmpty()) {
            return new WorkerStartupConfig(true, -1, -1,
                    "localhost", Constants.DEFAULT_RMI_PORT, grpc, grpcPort);
        }
        if (rest.size() == 2) {
            int bootstrapPort = parsePort(rest.get(1), "bootstrapPort");
            return new WorkerStartupConfig(true, -1, -1, rest.get(0), bootstrapPort, grpc, grpcPort);
        }
        if (rest.size() == 4) {
            try {
                int workerId = Integer.parseInt(rest.get(0));
                int rmiPort = Integer.parseInt(rest.get(1));
                int bootstrapPort = Integer.parseInt(rest.get(3));
                return new WorkerStartupConfig(false, workerId, rmiPort,
                        rest.get(2), bootstrapPort, grpc, grpcPort);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Worker id and ports must be integers: " + e.getMessage());
            }
        }
        throw new IllegalArgumentException(
                "Expected 0 args (auto), 2 args [bootstrapHost bootstrapPort], or 4 args (manual override),"
                        + " optionally preceded by --grpc [--grpc-port N].");
    }

    private static int parsePort(String raw, String name) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Worker id and ports must be integers: invalid " + name + " '" + raw + "'");
        }
    }

    // Scans upward from basePort until createRegistry succeeds; fails cleanly when exhausted.
    static LocalRegistry createRegistryOnFreePort(int basePort, int maxTries) throws RemoteException {
        RemoteException lastFailure = null;
        for (int i = 0; i < maxTries; i++) {
            int port = basePort + i;
            try {
                Registry registry = LocateRegistry.createRegistry(port);
                return new LocalRegistry(registry, port);
            } catch (RemoteException e) {
                // Port taken (or registry already exists): try the next one.
                lastFailure = e;
            }
        }
        throw new RemoteException("No free RMI port in range "
                + basePort + "-" + (basePort + maxTries - 1)
                + (lastFailure != null ? ": " + lastFailure.getMessage() : ""));
    }

    // Shared join path for auto and manual startup: connect both neighbour edges and adopt the peer's coordinator.
    private static void joinViaPeer(WorkerRemoteImpl self, BootstrapRemote directory, WorkerRemote peerStub,
            int workerId, String myHost, int rmiPort, String bootstrapHost, int bootstrapPort)
            throws Exception {
        // A null peer means we are first, so there is nothing to connect to.
        if (peerStub == null) {
            System.out.println("First worker in the network - no peer to connect to.");
            return;
        }
        // Ask the peer who it is, then resolve its address through the directory.
        int peerId = peerStub.getId();
        WorkerAddress peerAddress = directory.getWorkerAddress(peerId);
        // Fall back to the bootstrap own address if the directory lost that peer.
        String peerHost = peerAddress != null ? peerAddress.getRmiHost() : bootstrapHost;
        int peerPort = peerAddress != null ? peerAddress.getRmiPort() : bootstrapPort;

        // Forward edge: remember the peer locally.
        self.connectAsNeighbour(peerId, peerHost, peerPort);
        // Reverse edge: the successful call is the acknowledgement the link is up.
        try {
            peerStub.connectAsNeighbour(workerId, myHost, rmiPort);
            System.out.println("Connected to peer worker " + peerId + " (bidirectional).");
        } catch (Exception e) {
            System.out.println("Peer found but reverse connect failed: " + e.getMessage());
        }
        // Adopt an existing coordinator, so a late joiner neither waits nor forces an election.
        try {
            int peerCoordinator = peerStub.getCurrentCoordinatorId();
            if (peerCoordinator != -1) {
                // Adopted locally and never relayed: a re-broadcast would read as a new term.
                // Relaying would hand that coordinator a second full job budget mid-term.
                self.adoptCoordinator(peerCoordinator, adoptMessageId(workerId));
                System.out.println("Adopted coordinator " + peerCoordinator
                        + " from peer worker " + peerId + ".");
            }
        } catch (Exception e) {
            System.out.println("Could not read the peer's coordinator: " + e.getMessage());
        }
    }

    // Id for a locally adopted announcement; unique only within this node own space.
    private static long adoptMessageId(int workerId) {
        return ((long) workerId << 32) | ADOPT_MESSAGE_TAG;
    }

    // Console loop: shows the current view and lets a worker trigger an election by hand.
    private static void runMenu(WorkerRemoteImpl self, int workerId) {
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                int coordinatorId;
                try {
                    // A failed read means we cannot confirm a leader, so report it as unknown.
                    coordinatorId = self.getCurrentCoordinatorId();
                } catch (RemoteException e) {
                    coordinatorId = -1;
                }
                System.out.println();
                Set<Integer> neighbourIds;
                try {
                    // Local call, but the RMI signature forces checked-exception handling.
                    neighbourIds = self.getNeighbourIds();
                } catch (RemoteException e) {
                    neighbourIds = Set.of();
                }
                System.out.println("Worker " + workerId + " | coordinator=" + coordinatorId
                        + " | neighbours=" + neighbourIds);
                System.out.println("[1] Start Election  [2] Show Leader  [3] Exit");
                System.out.print("> ");
                // EOF on stdin ends the menu, which lets the caller shut the worker down.
                if (!scanner.hasNextLine()) {
                    return;
                }
                String choice = scanner.nextLine().trim();
                switch (choice) {
                    case "1" -> {
                        long electionId = self.startElection();
                        System.out.println("Election started (messageId=" + electionId + ").");
                    }
                    case "2" -> System.out.println("Current coordinator: " + coordinatorId);
                    case "3" -> {
                        System.out.println("Leaving - deregistering from Bootstrap.");
                        return;
                    }
                    default -> System.out.println("Unknown option: " + choice);
                }
            }
        }
    }
}
