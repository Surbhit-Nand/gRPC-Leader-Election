package com.cs324.failover;

import com.cs324.failover.proto.FetchReply;
import com.cs324.registry.proto.WorkerEndpoint;

import java.util.List;

/**
 * Where a takeover finds other replicas. The gRPC replicator is the production implementation;
 * the interface exists so the takeover's decisions can be tested against known peer states
 * instead of a live cluster.
 */
public interface CheckpointSource {

    /** Workers that are reachable and ready to answer, this one excluded. */
    List<WorkerEndpoint> readyPeers();

    /**
     * One peer's replica.
     *
     * @return the peer's entries, or null when the peer did not answer at all. The difference
     *         matters: an empty reply means "nothing to recover", while null means the worker is
     *         gone, and a takeover must treat a registered-but-silent worker as gone.
     */
    FetchReply fetch(WorkerEndpoint peer);
}