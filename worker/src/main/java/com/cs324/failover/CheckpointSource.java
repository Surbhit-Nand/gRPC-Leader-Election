package com.cs324.failover;

import com.cs324.failover.proto.FetchReply;
import com.cs324.registry.proto.WorkerEndpoint;

import java.util.List;

// Provide peer checkpoints for recovery, with a replaceable source for isolated tests.
public interface CheckpointSource {

    // Workers that are reachable and ready to answer, this one excluded.
    List<WorkerEndpoint> readyPeers();

    // Return peer entries, an empty list for no jobs, or null when the peer does not answer.
    FetchReply fetch(WorkerEndpoint peer);
}