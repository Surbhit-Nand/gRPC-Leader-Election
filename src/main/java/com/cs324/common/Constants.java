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

    // ---- Coordinator failover (Member 2) ----

    // The legacy RMI heartbeat is kept only as a reference implementation: one detector at a
    // time, so the gRPC health monitor owns failure detection and this is off by default.
    public static final boolean LEGACY_RMI_HEARTBEAT_ENABLED = false;

    // How often a worker probes the coordinator it currently believes in, over gRPC.
    public static final int FAILOVER_HEARTBEAT_INTERVAL_MS = 1000;

    // Per-call deadline for one heartbeat. Kept well under the interval so a slow
    // coordinator shows up as a missed probe instead of overlapping the next one.
    public static final int FAILOVER_HEARTBEAT_DEADLINE_MS = 600;

    // A reply slower than this is ALIVE but SUSPECT: it proved the peer is up, so no election.
    public static final int FAILOVER_HEARTBEAT_SLOW_MS = 400;

    // Consecutive misses before a coordinator counts as FAILED rather than SUSPECT.
    // Each miss costs at most interval + deadline, so three misses detect a dead coordinator
    // in roughly three to five seconds: fast enough to demo, slow enough to survive a GC pause.
    public static final int FAILOVER_HEARTBEAT_FAILURE_THRESHOLD = 3;

    // No heartbeat at all for this long is failure regardless of the miss counter, so a peer
    // that stops answering mid-window is not carried by an earlier success.
    public static final int FAILOVER_HEARTBEAT_SILENCE_MS = 8000;

    // Random delay before an automatic failover wave, so every survivor does not wave at once.
    public static final int FAILOVER_ELECTION_JITTER_MS = 400;

    // Two failed coordinators within this window are treated as one failure, which stops a
    // healing cluster from running a wave per survivor.
    public static final int FAILOVER_SUPPRESSION_MS = 3000;

    // Deadline for making an accepted job durable on at least one peer before the client is
    // acknowledged. Bounded so a peerless cluster still runs, with a logged warning.
    public static final int FAILOVER_REPLICATION_DEADLINE_MS = 1000;

    // Per-call deadlines for the replication and takeover RPCs.
    public static final int FAILOVER_REPLICATION_RPC_DEADLINE_MS = 700;
    public static final int FAILOVER_FETCH_DEADLINE_MS = 1500;

    // Finished jobs are dropped from a replica past this count, so replication stays bounded.
    public static final int FAILOVER_CHECKPOINT_RETENTION = 256;

    // Floor for the deadline of a restored job: a job that was nearly out of time still gets
    // a fair chance to finish after its coordinator was replaced.
    public static final long FAILOVER_MIN_REMAINING_MS = 30_000;
}
