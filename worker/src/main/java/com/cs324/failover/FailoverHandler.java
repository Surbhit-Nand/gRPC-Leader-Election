package com.cs324.failover;

import com.cs324.failover.proto.HeartbeatReply;

// Worker-owned state is accessed through these failover RPC handlers.
public interface FailoverHandler {

    // Answers one liveness probe with this worker's own leadership view, so the follower can tell a busy coordinator from a deposed one.
    HeartbeatReply heartbeat(int fromWorkerId, long followerTerm, int followerCoordinatorId);

    // Folds replicated entries from one coordinator. Returns the highest sequence held.
    long acceptEntries(int coordinatorId, long term, java.util.List<com.cs324.failover.proto.CheckpointEntry> entries);

    // This replica's recoverable jobs, as entries the caller can fold.
    java.util.List<com.cs324.failover.proto.CheckpointEntry> snapshots();
}