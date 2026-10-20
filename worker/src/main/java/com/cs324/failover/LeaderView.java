package com.cs324.failover;

/**
 * Decides whether a peer's reported leadership view should replace this worker's own.
 *
 * <p>A heartbeat reply can contradict what we believe in two ways, and both mean the cluster has
 * moved on rather than that anything is wrong:
 *
 * <ul>
 *   <li>the peer reports a <b>newer term</b> - a newer election has concluded;</li>
 *   <li>the peer reports the <b>same term</b> but says it is <b>not</b> the coordinator and names
 *       someone else - the worker we thought was leading has already stood down.</li>
 * </ul>
 *
 * <p>Both are answered by adopting the peer's view, not by starting an election: the cluster has
 * already decided, and a wave would only add noise. An older term, or a view that agrees with
 * ours, changes nothing.
 */
public final class LeaderView {

    /** The outcome: whether to adopt, and what to adopt. */
    public record Decision(boolean adopt, int coordinatorId, long term) {

        public static final Decision KEEP = new Decision(false, -1, 0);
    }

    private LeaderView() {}

    /**
     * @param ourTerm          this worker's current leadership generation
     * @param ourCoordinatorId the coordinator this worker believes in
     * @param peerTerm         the term the peer reported
     * @param peerCoordinatorId the coordinator the peer believes in
     * @param peerIsCoordinator whether the peer says it is the coordinator itself
     */
    public static Decision decide(long ourTerm, int ourCoordinatorId, long peerTerm,
            int peerCoordinatorId, boolean peerIsCoordinator) {
        // A peer that knows no leader tells us nothing we can adopt.
        if (peerCoordinatorId < 1) {
            return Decision.KEEP;
        }
        if (peerTerm > ourTerm) {
            return new Decision(true, peerCoordinatorId, peerTerm);
        }
        if (peerTerm < ourTerm) {
            return Decision.KEEP;
        }
        // Same generation: only a contradiction is worth acting on. Adopting the same
        // coordinator we already hold would be a no-op that could still reset a term budget.
        if (!peerIsCoordinator && peerCoordinatorId != ourCoordinatorId) {
            return new Decision(true, peerCoordinatorId, peerTerm);
        }
        return Decision.KEEP;
    }
}