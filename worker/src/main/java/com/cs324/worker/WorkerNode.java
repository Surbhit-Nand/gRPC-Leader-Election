package com.cs324.worker;

import com.cs324.common.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

// Entry point for a Worker node: auto-assigns id + port, registers, joins a peer, serves elections and jobs.
public class WorkerNode {


    // Parsed startup request. Workers always take an automatically allocated id.
    record WorkerStartupConfig(String bootstrapHost, int bootstrapGrpcPort) {}


    public static void main(String[] args) {
        WorkerStartupConfig config = parseArgs(args);
        var directory = new com.cs324.computation.RegistryClient(config.bootstrapHost(), config.bootstrapGrpcPort());
        WorkerRuntime self = null;
        int workerId = -1;
        boolean failed = false;
        var stopped = new java.util.concurrent.atomic.AtomicBoolean();
        try {
            workerId = directory.registerWorkerAuto("localhost", 0).workerId();
            self = new WorkerRuntime(workerId);
            self.startProgressServer();
            self.setBootstrapReference(directory, "localhost");
            self.enableGrpcJobs(config.bootstrapHost(), config.bootstrapGrpcPort(), "localhost", 0);
            // Refresh after publishing: concurrent joiners may not have been ready at registration.
            // Join a random live worker instead of the lowest ID, so the topology does not
            // collapse into a star rooted at Worker 1. Repair still learns the parent's
            // neighbours at connect time, so a survivor can chain onward when that parent dies.
            var candidates = new ArrayList<>(directory.workers());
            Collections.shuffle(candidates);
            for (var endpoint : candidates) {
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
            final WorkerRuntime worker = self;
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

    private static void stopGrpcWorker(WorkerRuntime worker, int id,
            com.cs324.computation.RegistryClient directory, java.util.concurrent.atomic.AtomicBoolean stopped) {
        if (!stopped.compareAndSet(false, true)) return;
        notifyNeighboursOfLeaving(worker, id);
        if (worker != null) worker.shutdown();
        try { if (id > 0) directory.deregisterWorker(id); }
        catch (Exception ignored) { /* A crashed bootstrap cannot acknowledge deregistration. */ }
        directory.close();
    }
    // Give neighbours this worker links so they can reconnect after a graceful exit.
    private static void notifyNeighboursOfLeaving(WorkerRuntime self, int workerId) {
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

    // Usage: WorkerNode [bootstrapHost] [bootstrapGrpcPort] [--grpc-port N]; the flag overrides the port.
    static WorkerStartupConfig parseArgs(String[] args) {
        List<String> rest = new ArrayList<>(Arrays.asList(args));
        Integer flagPort = null;
        int portFlag = rest.indexOf("--grpc-port");
        if (portFlag != -1) {
            if (portFlag + 1 >= rest.size()) {
                throw new IllegalArgumentException("Missing value for --grpc-port.");
            }
            flagPort = parsePort(rest.remove(portFlag + 1), "grpcPort");
            rest.remove(portFlag);
        }
        if (rest.isEmpty()) {
            return new WorkerStartupConfig("localhost",
                    flagPort != null ? flagPort : Constants.DEFAULT_GRPC_PORT);
        }
        if (rest.size() != 2) {
            throw new IllegalArgumentException(
                    "Expected 0 args or 2 args [bootstrapHost bootstrapGrpcPort],"
                            + " optionally with --grpc-port N.");
        }
        int positionalPort = parsePort(rest.get(1), "bootstrapGrpcPort");
        return new WorkerStartupConfig(rest.get(0),
                flagPort != null ? flagPort : positionalPort);
    }

    private static int parsePort(String raw, String name) {
        try {
            int port = Integer.parseInt(raw);
            if (port < 0 || port > 65535) {
                throw new NumberFormatException("out of range");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid port for " + name + ": '" + raw + "'");
        }
    }

    // Console loop: shows the current view and lets a worker trigger an election by hand.
    private static void runMenu(WorkerRuntime self, int workerId) {
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                int coordinatorId;
                try {
                    // A failed read means we cannot confirm a leader, so report it as unknown.
                    coordinatorId = self.getCurrentCoordinatorId();
                } catch (RuntimeException e) {
                    coordinatorId = -1;
                }
                System.out.println();
                Set<Integer> neighbourIds;
                try {
                    // Read the local topology for the menu.
                    neighbourIds = self.getNeighbourIds();
                } catch (RuntimeException e) {
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
