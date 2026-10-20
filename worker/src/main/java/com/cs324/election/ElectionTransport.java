package com.cs324.election;

import java.util.Set;

/**
 * Carries one election hop between neighbouring workers.
 *
 * <p>Every method answers "did the peer accept this message?" instead of throwing,
 * because the caller's response to a failed hop is the same whether the peer is
 * dead, unreachable or slow: drop the edge from the pending set and rewire past
 * it. Transport errors are therefore data here, not exceptions.
 */
public interface ElectionTransport extends AutoCloseable {

    /** One wave hop, forwarded to a neighbour that is not the wave's parent. */
    boolean sendElection(int peerId, int originId, int candidateId, int candidateJac, long messageId, long term,
            Set<Integer> voters);

    /** One subtree answer, echoed back toward the wave origin. */
    boolean sendEcho(int peerId, long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac,
            long term, Set<Integer> voters);

    /** The winner's verdict, relayed outward by every worker that hears it. */
    boolean sendAnnouncement(int peerId, int coordinatorId, long messageId, long term);

    /** Releases cached channels and stops accepting work. Idempotent. */
    @Override
    void close();
}