package com.cs324.worker;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.Constants;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerRemote;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

// Entry point for a Worker node: registers, joins a random peer, then serves elections and jobs.
public class WorkerNode {

    // Tag for a locally generated adoption id, in a range no election counter reaches.
    private static final long ADOPT_MESSAGE_TAG = 0x5AD0L;

        // All four arguments are mandatory: without them there is no cluster to join.
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: WorkerNode <workerId> <rmiPort> <bootstrapHost> <bootstrapPort>");
            System.exit(1);
        }

        int workerId;
        int rmiPort;
        int bootstrapPort;
        String bootstrapHost = args[2];
        try {
            workerId = Integer.parseInt(args[0]);
            rmiPort = Integer.parseInt(args[1]);
            bootstrapPort = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.out.println("Worker id and ports must be integers: " + e.getMessage());
            System.exit(1);
            return;
        }

        // localhost is right for a single-host demo; a real hostname is needed for multi-host.
        String myHost = "localhost";

        // Assigned here so the shutdown hook below can reach the same instance.
        WorkerRemoteImpl self = null;
        Registry localRegistry = null;
        // Array holder so the hook can read a bootstrap that may never have been created.
        final BootstrapRemote[] bootstrap = new BootstrapRemote[1];
        final int idForHook = workerId;
        try {
            // Every worker owns its own registry, so each node needs a distinct port.
            localRegistry = LocateRegistry.createRegistry(rmiPort);
            self = new WorkerRemoteImpl(workerId);
            // Peers look this worker up under exactly this name.
            localRegistry.rebind("Worker" + workerId, self);
            System.out.println("Worker " + workerId + " bound as Worker" + workerId + " on port " + rmiPort);

            Registry bootstrapRegistry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
            bootstrap[0] = (BootstrapRemote) bootstrapRegistry.lookup("BootstrapRegistry");

            // Registration also returns the one peer to join, or null if we are first.
            WorkerRemote peerStub = bootstrap[0].registerWorker(workerId, myHost, rmiPort);
                // A null peer means we are first, so there is nothing to connect to.
            if (peerStub == null) {
                System.out.println("First worker in the network - no peer to connect to.");
            } else {
                // Ask the peer who it is, then resolve its address through the directory.
                int peerId = peerStub.getId();
                WorkerAddress peerAddress = bootstrap[0].getWorkerAddress(peerId);
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
            // Heartbeats start last: a silent coordinator is what triggers a new election.
            self.setBootstrapReference(bootstrap[0], myHost);
        } catch (Exception e) {
            System.out.println("Failed to start Worker Node: " + e.getMessage());
            System.exit(1);
        }

        // Final copies so the hook and the startup thread share one instance.
        final WorkerRemoteImpl selfForHook = self;
        final WorkerRemoteImpl selfForStartup = self;
        if (selfForStartup != null) {
            // The cluster elects itself: after a grace period, a worker with no coordinator starts a wave.
            // The delay lets peers adopt the first leader, so startup costs one election, not N.
            Thread startupElection = new Thread(() -> {
                try {
                    Thread.sleep(Constants.STARTUP_ELECTION_DELAY_MS);
                    // Still unknown after the grace period, so this node must start the wave.
                    if (selfForStartup.getCurrentCoordinatorId() == -1) {
                        System.out.println("No coordinator known after startup - starting an election.");
                        selfForStartup.startElection();
                    } else {
                        System.out.println("Coordinator " + selfForStartup.getCurrentCoordinatorId()
                                + " already known - no startup election needed.");
                    }
                } catch (Exception e) {
                    System.out.println("Startup election skipped: " + e.getMessage());
                }
            }, "worker-startup-election-" + workerId);
            // Daemon: must never hold the JVM open on its own.
            startupElection.setDaemon(true);
            startupElection.start();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                // Pool first: leaked pool threads are what stop a JVM from exiting.
                if (selfForHook != null) {
                    selfForHook.shutdown();
                }
            } catch (Exception ignored) {
                // best effort on shutdown
            }
            try {
                if (bootstrap[0] != null) {
                    bootstrap[0].deregisterWorker(idForHook);
                }
            } catch (Exception ignored) {
                // best effort on shutdown
            }
            try {
                if (selfForHook != null) {
                    java.rmi.server.UnicastRemoteObject.unexportObject(selfForHook, true);
                }
            } catch (Exception ignored) {
                // already unexported
            }
        }));

        runMenu(self, workerId);

        // RMI export holds non-daemon threads open, so returning would never end the JVM.
        System.exit(0);
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
                System.out.println("Worker " + workerId + " | coordinator=" + coordinatorId
                        + " | neighbours=" + self.getNeighbourIds());
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
