package com.cs324.common;

// Shared constants for the bootstrap, worker, and client modules.
public class Constants {

    // Default RMI registry port; every node in one demo must agree on it.
    public static final int DEFAULT_RMI_PORT = 1099;

    // A coordinator serves 5 client jobs per term; JAC still increments per assignment.
    public static final int JOBS_PER_TERM = 5;

    // How often a worker probes the coordinator it currently believes in.
    public static final int HEARTBEAT_INTERVAL_MS = 2000;

    // Settle time after startup, so an already-formed cluster never holds a pointless election.
    public static final int STARTUP_ELECTION_DELAY_MS = 3000;
}
