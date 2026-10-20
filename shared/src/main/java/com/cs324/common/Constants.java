package com.cs324.common;

// Shared constants for the bootstrap, worker, and client modules.
public class Constants {

    // Default Bootstrap gRPC port (RegistryService).
    public static final int DEFAULT_GRPC_PORT = 50051;

    // Default local callback port for client result delivery.
    public static final int DEFAULT_CLIENT_PORT = 1199;

    // A coordinator serves 5 client jobs per term; JAC still increments per assignment.
    public static final int JOBS_PER_TERM = 5;

    // How often a worker probes the coordinator it currently believes in.
    public static final int HEARTBEAT_INTERVAL_MS = 2000;

    // Deadline for one election hop over gRPC. It bounds a single forward, not the whole
    // election: a wave crosses several hops, so this only has to exceed one local round trip.
    public static final int ELECTION_RPC_DEADLINE_MS = 1500;

    // Settle time after startup, so an already-formed cluster never holds a pointless election.
    public static final int STARTUP_ELECTION_DELAY_MS = 3000;

    // ---- Coordinator failover (Member 2) ----

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
    // acknowledged. Without confirmation, submission fails with retryable UNAVAILABLE.
    public static final int FAILOVER_REPLICATION_DEADLINE_MS = 1000;

    // Per-call deadlines for the replication and takeover RPCs.
    public static final int FAILOVER_REPLICATION_RPC_DEADLINE_MS = 700;
    public static final int FAILOVER_FETCH_DEADLINE_MS = 1500;

    // ---- Majority quorum (split-brain prevention) ----

    // How often a leader checks that it can still reach a majority of ready workers.
    public static final int QUORUM_CHECK_INTERVAL_MS = 1000;

    // Per-peer status deadline for one quorum check. Short, because a partition must be
    // detected quickly rather than waiting on a peer that will never answer.
    public static final int QUORUM_PROBE_DEADLINE_MS = 600;

    // Consecutive failed checks before a leader steps down, so one slow probe is tolerated.
    public static final int QUORUM_LOSS_THRESHOLD = 2;

    // New submissions are accepted only within this window of the last confirmed majority,
    // so a leader that has just lost contact cannot keep serving indefinitely.
    public static final int QUORUM_LEASE_MS = 2500;

    // Finished jobs are dropped from a replica past this count, so replication stays bounded.
    public static final int FAILOVER_CHECKPOINT_RETENTION = 256;

    // Floor for the deadline of a restored job: a job that was nearly out of time still gets
    // a fair chance to finish after its coordinator was replaced.
    public static final long FAILOVER_MIN_REMAINING_MS = 30_000;

    // ---- Bootstrap membership leases and retention ----

    // Workers refresh their directory entry this often. Publication is idempotent, so this
    // is also the liveness heartbeat for the bootstrap registry.
    public static final int BOOTSTRAP_LEASE_INTERVAL_MS = 5_000;

    // A worker that stops refreshing its entry is removed after this silence. The margin over
    // the refresh interval tolerates one delayed RPC without dropping a live worker.
    public static final int BOOTSTRAP_WORKER_TTL_MS = 15_000;

    // Retain enough recent jobs and tasks for duplicate-submission and retry windows while
    // keeping a long-lived coordinator's memory bounded.
    public static final int JOB_SUBMISSION_RETENTION = 1_024;
    public static final int JOB_TASK_RETENTION = 4_096;

    // Election waves and coordinator messages are only deduplicated for recent history.
    public static final int ELECTION_STATE_RETENTION = 512;
    public static final int ELECTION_MESSAGE_RETENTION = 4_096;
}
