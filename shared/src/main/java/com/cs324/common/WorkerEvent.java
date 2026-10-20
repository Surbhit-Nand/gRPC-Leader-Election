package com.cs324.common;

// Sequenced worker events let the GUI fetch only new entries from a bounded buffer.
public record WorkerEvent(long seq, int workerId, long epochMillis, String kind, String detail) {

    // Election wave originated here.
    public static final String ELECTION_STARTED = "ELECTION_STARTED";
    // This worker concluded a wave and broadcast the winner.
    public static final String COORDINATOR_ANNOUNCED = "COORDINATOR_ANNOUNCED";
    // Adopted a coordinator from a peer without an election (late join).
    public static final String COORDINATOR_ADOPTED = "COORDINATOR_ADOPTED";
    // Bidirectional neighbour edge added; detail names the cause (join/repair).
    public static final String NEIGHBOUR_ADDED = "NEIGHBOUR_ADDED";
    // Neighbour edge pruned; detail names the cause (unreachable/leave).
    public static final String NEIGHBOUR_REMOVED = "NEIGHBOUR_REMOVED";
    // Coordinator handed one slice out; detail names job and target.
    public static final String TASK_DISPATCHED = "TASK_DISPATCHED";
    // A dispatched slice arrived here for local computation.
    public static final String TASK_RECEIVED = "TASK_RECEIVED";
    public static final String WORKER_FAILED = "WORKER_FAILED";
    public static final String TASK_REASSIGNED = "TASK_REASSIGNED";
    public static final String TASK_COMPLETED = "TASK_COMPLETED";
    // Coordinator answered late or not at all, but is still counted as active.
    public static final String COORDINATOR_SUSPECTED = "COORDINATOR_SUSPECTED";
    // Coordinator missed enough probes to be declared dead; this starts a failover.
    public static final String COORDINATOR_FAILED = "COORDINATOR_FAILED";
    // A surviving worker became coordinator and began merging the replicated job log.
    public static final String FAILOVER_STARTED = "FAILOVER_STARTED";
    // The merge finished; the recovered jobs are running again.
    public static final String FAILOVER_COMPLETED = "FAILOVER_COMPLETED";
    // One replicated job was rebuilt and resumed after a coordinator failure.
    public static final String JOB_RECOVERED = "JOB_RECOVERED";
    // A checkpoint entry was written to at least one peer.
    public static final String CHECKPOINT_REPLICATED = "CHECKPOINT_REPLICATED";
    // This worker saw a newer term and gave up leadership.
    public static final String LEADERSHIP_STEP_DOWN = "LEADERSHIP_STEP_DOWN";
    // A wave reached fewer than a majority of ready workers, so no winner was announced.
    public static final String ELECTION_NO_QUORUM = "ELECTION_NO_QUORUM";
    // The leader lost its majority lease and stopped serving new jobs.
    public static final String QUORUM_LOST = "QUORUM_LOST";
    // A replication or election hop failed; the recovery continues without it.
    public static final String FAILOVER_DETAIL = "FAILOVER_DETAIL";
    public static final String ELECTION_TRANSPORT = "ELECTION_TRANSPORT";
    // Clean exit: handed the neighbour set over before deregistering.
    public static final String LEAVING = "LEAVING";
}
