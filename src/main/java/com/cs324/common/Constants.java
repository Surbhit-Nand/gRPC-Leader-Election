package com.cs324.common;

// Shared constants for the bootstrap, worker, and client modules.
public class Constants {

    // Default RMI registry port; every node in one demo must agree on it.
    public static final int DEFAULT_RMI_PORT = 1099;

    // Default Bootstrap gRPC port (RegistryService); RMI keeps its own port above.
    public static final int DEFAULT_GRPC_PORT = 50051;

    // A coordinator serves 5 client jobs per term; JAC still increments per assignment.
    public static final int JOBS_PER_TERM = 5;

    // How often a worker probes the coordinator it currently believes in.
    public static final int HEARTBEAT_INTERVAL_MS = 2000;

    // Deadline for one election hop over gRPC. It bounds a single forward, not the whole
    // election: a wave crosses several hops, so this only has to exceed one local round trip.
    public static final int ELECTION_RPC_DEADLINE_MS = 1500;

    // Settle time after startup, so an already-formed cluster never holds a pointless election.
    public static final int STARTUP_ELECTION_DELAY_MS = 3000;

    // Base RMI port for auto-assigned workers; scan upward until createRegistry succeeds.
    public static final int WORKER_BASE_RMI_PORT = 3500;

    // How many ports to probe starting from the base before giving up.
    public static final int WORKER_MAX_PORT_TRIES = 1000;
}
