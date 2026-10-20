package com.cs324.failover;

// Decide whether a peer reports a leadership view that this worker should adopt.
public final class LeaderView {

    // The outcome: whether to adopt, and what to adopt.
    public record Decision(boolean adopt, int coordinatorId, long term) {

        public static final Decision KEEP = new Decision(false, -1, 0);
    }

    private LeaderView() {}

    // Compare our term and leader with the peer reply before adopting its view.
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
        // Ignore an unchanged leadership view so it cannot reset the term budget.
        if (!peerIsCoordinator && peerCoordinatorId != ourCoordinatorId) {
            return new Decision(true, peerCoordinatorId, peerTerm);
        }
        return Decision.KEEP;
    }
}