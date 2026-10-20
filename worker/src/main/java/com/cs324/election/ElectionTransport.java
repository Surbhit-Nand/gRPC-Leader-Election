package com.cs324.election;

import java.util.Set;

// Send election messages to neighbours; false tells the caller to repair a failed edge.
public interface ElectionTransport extends AutoCloseable {

    // One wave hop, forwarded to a neighbour that is not the wave's parent.
    boolean sendElection(int peerId, int originId, int candidateId, int candidateJac, long messageId, long term,
            Set<Integer> voters);

    // One subtree answer, echoed back toward the wave origin.
    boolean sendEcho(int peerId, long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac,
            long term, Set<Integer> voters);

    // The winner's verdict, relayed outward by every worker that hears it.
    boolean sendAnnouncement(int peerId, int coordinatorId, long messageId, long term);

    // Releases cached channels and stops accepting work. Idempotent.
    @Override
    void close();
}