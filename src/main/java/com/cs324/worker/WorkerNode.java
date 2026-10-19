package com.cs324.worker;

import com.cs324.common.Constants;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

// Entry point for a Worker node: auto-assigns id + port, registers, joins a peer, serves elections and jobs.
public class WorkerNode {


    // Parsed startup request: either fully automatic or a hidden manual override (tests/debug).
    // Startup always uses gRPC. Legacy contact-port arguments remain accepted.
    // --grpc-port selects the bootstrap gRPC port.
    record WorkerStartupConfig(boolean auto, int workerId, int rmiPort,
            String bootstrapHost, int bootstrapPort, boolean grpc, int bootstrapGrpcPort) {}


    public static void main(String[] args) {
        WorkerStartupConfig config = parseArgs(args);
        var directory = new com.cs324.computation.RegistryClient(config.bootstrapHost(), config.bootstrapGrpcPort());
        WorkerRemoteImpl self = null;
        int workerId = -1;
        boolean failed = false;
        var stopped = new java.util.concurrent.atomic.AtomicBoolean();
        try {
            // The old contact-port argument is retained for launch-file compatibility;
            // only published grpc_port endpoints are ever contacted by this process.
            if (config.auto()) workerId = directory.registerWorkerAuto("localhost", 0).workerId();
            else {
                directory.registerWorker(config.workerId(), "localhost", 0);
                workerId = config.workerId();
            }
            self = new WorkerRemoteImpl(workerId, false);
            self.startProgressServer();
            self.setBootstrapReference(directory, "localhost");
            self.enableGrpcJobs(config.bootstrapHost(), config.bootstrapGrpcPort(), "localhost", 0);
            // Refresh after publishing: concurrent joiners may not have been ready at registration.
            for (var endpoint : directory.workers()) {
                if (endpoint.getWorkerId() == workerId) continue;
                try {
                    var peer = new com.cs324.computation.GrpcWorkerClient(endpoint);
                    self.connectGrpcNeighbour(endpoint.getWorkerId());
                    peer.connectAsNeighbour(workerId, "localhost", self.getProgressPort());
                    var view = peer.status();
                    self.adoptGrpcCoordinator(view.getCoordinatorId(), view.getTerm());
                    break;
                } catch (Exception unavailable) {
                    System.out.println("Join peer unavailable; trying the next published worker.");
                }
            }
            final WorkerRemoteImpl worker = self;
            final int assignedId = workerId;
            Runtime.getRuntime().addShutdownHook(new Thread(() ->
                    stopGrpcWorker(worker, assignedId, directory, stopped)));
            Thread startup = new Thread(() -> {
                try {
                    Thread.sleep(Constants.STARTUP_ELECTION_DELAY_MS);
                    if (!worker.isShutdown() && worker.getCurrentCoordinatorId() == -1) worker.startElection();
                } catch (Exception e) { System.out.println("Startup election skipped: " + e.getMessage()); }
            }, "worker-startup-election-" + workerId);
            startup.setDaemon(true);
            startup.start();
            runMenu(self, workerId);
        } catch (Exception e) {
            failed = true;
            System.out.println("Failed to start Worker Node: " + e.getMessage());
        } finally {
            stopGrpcWorker(self, workerId, directory, stopped);
        }
        if (failed) System.exit(1);
    }

    private static void stopGrpcWorker(WorkerRemoteImpl worker, int id,
            com.cs324.computation.RegistryClient directory, java.util.concurrent.atomic.AtomicBoolean stopped) {
        if (!stopped.compareAndSet(false, true)) return;
        notifyNeighboursOfLeaving(worker, id);
        if (worker != null) worker.shutdown();
        try { if (id > 0) directory.deregisterWorker(id); }
        catch (Exception ignored) { /* A crashed bootstrap cannot acknowledge deregistration. */ }
        directory.close();
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

    // or 4 args (hidden manual override for tests/debug). Anything else is rejected.
    // Leading flags: --grpc is a compatibility no-op;
    // --grpc-port N overrides the Bootstrap gRPC port (default 50051, implies --grpc).
    static WorkerStartupConfig parseArgs(String[] args) {
        List<String> rest = new ArrayList<>(Arrays.asList(args));
        rest.remove("--grpc"); // Accepted for compatibility: gRPC is now unconditional.
        boolean grpc = true;
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
