package com.cs324.failover;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two rules that decide *when* the cluster acts: one election per failure, and one view per
 * term. Both are pure decisions, so both are tested without a cluster.
 */
class FailureGateTest {

    private final AtomicLong clock = new AtomicLong(1_000);
    private final FailureGate gate = new FailureGate(3_000, clock::get);

    @Test
    void theFirstDetectionOfAFailureMayElect() {
        assertTrue(gate.shouldElect(4));
    }

    @Test
    void otherWorkersSeeingTheSameDeathInTheWindowDoNotElectAgain() {
        assertTrue(gate.shouldElect(4));
        // Same coordinator, inside the window: one death, one wave.
        assertFalse(gate.shouldElect(4));
        clock.addAndGet(2_999);
        assertFalse(gate.shouldElect(4), "still inside the window");
    }

    @Test
    void theSameWorkerFailingAgainMuchLaterIsANewFailure() {
        // The regression this guards: keying suppression on the coordinator id alone would make
        // a second, unrelated death of the same worker id undetectable for ever, leaving the
        // cluster with a dead coordinator and no election to replace it.
        assertTrue(gate.shouldElect(4));
        clock.addAndGet(60_000);
        assertTrue(gate.shouldElect(4), "a death an hour later must still be able to elect");
    }

    @Test
    void aDifferentCoordinatorFailingIsNeverSuppressedByThePreviousOne() {
        assertTrue(gate.shouldElect(4));
        assertTrue(gate.shouldElect(7), "a different coordinator is a different failure");
    }

    @Test
    void aWindowOfZeroSuppressesNothing() {
        FailureGate open = new FailureGate(0, clock::get);
        assertTrue(open.shouldElect(4));
        assertTrue(open.shouldElect(4));
    }
}

/** Whether a peer's reported view should replace ours. */
class LeaderViewTest {

    @Test
    void aNewerTermIsAdopted() {
        LeaderView.Decision decision = LeaderView.decide(3, 1, 4, 2, false);
        assertTrue(decision.adopt());
        assertEquals(2, decision.coordinatorId());
        assertEquals(4, decision.term());
    }

    @Test
    void anOlderTermIsIgnoredSoAZombieCannotPullTheClusterBack() {
        assertFalse(LeaderView.decide(5, 2, 3, 9, true).adopt(),
                "a deposed coordinator's view must never win");
    }

    @Test
    void aPeerThatDisownsLeadershipAtTheSameTermIsFollowed() {
        // The case the docs promise: we probe worker 4 believing it leads, and it says it is not
        // the coordinator any more and names worker 6.
        LeaderView.Decision decision = LeaderView.decide(3, 4, 3, 6, false);
        assertTrue(decision.adopt());
        assertEquals(6, decision.coordinatorId());
    }

    @Test
    void agreementAtTheSameTermChangesNothing() {
        assertFalse(LeaderView.decide(3, 4, 3, 4, true).adopt());
        assertFalse(LeaderView.decide(3, 4, 3, 4, false).adopt(),
                "the same leader reported twice is not a contradiction");
    }

    @Test
    void aPeerThatKnowsNoLeaderIsIgnored() {
        assertFalse(LeaderView.decide(3, 4, 3, -1, false).adopt());
        assertFalse(LeaderView.decide(3, 4, 9, -1, false).adopt());
    }

    @Test
    void aPeerClaimingLeadershipOverSomeoneElseAtTheSameTermIsNotFollowed() {
        // Worker 8 claims it is the coordinator at our term, but names someone else as the
        // leader. That is incoherent, so it must not overwrite a working belief.
        assertFalse(LeaderView.decide(3, 4, 3, 6, true).adopt());
    }
}