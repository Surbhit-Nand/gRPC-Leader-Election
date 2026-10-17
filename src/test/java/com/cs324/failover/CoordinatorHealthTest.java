package com.cs324.failover;

import com.cs324.common.Constants;
import com.cs324.common.WorkerEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The liveness rule, with no network in it: what counts as active, what counts as slow, and
 * what - and only what - is allowed to start an election.
 */
class CoordinatorHealthTest {

    private static final int THRESHOLD = 3;
    private static final long SLOW_MS = 400;
    private static final long SILENCE_MS = 8_000;
    private static final long NOW = 1_000_000L;

    private CoordinatorHealth health() {
        return new CoordinatorHealth(THRESHOLD, SLOW_MS, SILENCE_MS);
    }

    @Test
    void aTimelyAnswerIsAliveAndKeepsTheLastSuccess() {
        CoordinatorHealth health = health();
        assertEquals(CoordinatorHealth.State.ALIVE, health.recordSuccess(3, 50, NOW));
        assertEquals(0, health.consecutiveMisses());
    }

    @Test
    void aSlowAnswerIsSuspectButNotFailedSoNoElectionIsAllowed() {
        CoordinatorHealth health = health();
        // Answered, but slower than the threshold: proof of life, not proof of death.
        assertEquals(CoordinatorHealth.State.SUSPECT, health.recordSuccess(3, SLOW_MS + 100, NOW));
        assertEquals(CoordinatorHealth.State.SUSPECT, health.evaluate(NOW + 100));
    }

    @Test
    void oneMissIsSuspectAndStillNotAFailure() {
        CoordinatorHealth health = health();
        assertEquals(CoordinatorHealth.State.SUSPECT, health.recordMiss(3, NOW));
        assertEquals(1, health.consecutiveMisses());
    }

    @Test
    void theRunOfMissesThatTripsTheThresholdIsTheOnlyThingThatFails() {
        CoordinatorHealth health = health();
        health.recordSuccess(3, 10, NOW);
        CoordinatorHealth.State previous = health.recordMiss(3, NOW + 1000);
        assertEquals(CoordinatorHealth.State.SUSPECT, previous);
        previous = health.recordMiss(3, NOW + 2000);
        assertEquals(CoordinatorHealth.State.SUSPECT, previous);
        assertFalse(health.justFailed(previous), "two misses is still suspicion");

        CoordinatorHealth.State after = health.recordMiss(3, NOW + 3000);
        assertEquals(CoordinatorHealth.State.FAILED, after);
        assertTrue(health.justFailed(previous), "the transition into FAILED is the election trigger");
    }

    @Test
    void aRecoveryFromSuspectReturnsToAliveWithoutAnythingBeingElected() {
        CoordinatorHealth health = health();
        health.recordMiss(3, NOW);
        assertEquals(CoordinatorHealth.State.SUSPECT, health.state());
        assertEquals(CoordinatorHealth.State.ALIVE, health.recordSuccess(3, 20, NOW + 1000));
        assertEquals(0, health.consecutiveMisses());
    }

    @Test
    void silenceLongerThanTheWindowFailsEvenAfterASuccessfulAnswer() {
        CoordinatorHealth health = health();
        health.recordSuccess(3, 10, NOW);
        // One miss inside the threshold, then a long quiet patch: the old success cannot vouch.
        health.recordMiss(3, NOW + 1000);
        assertEquals(CoordinatorHealth.State.SUSPECT, health.evaluate(NOW + SILENCE_MS - 1));
        assertEquals(CoordinatorHealth.State.FAILED, health.evaluate(NOW + SILENCE_MS + 1));
    }

    @Test
    void pointingAtANewCoordinatorForgetsTheOldVerdict() {
        CoordinatorHealth health = health();
        health.recordSuccess(3, 10, NOW);
        health.recordMiss(3, NOW);
        assertEquals(CoordinatorHealth.State.SUSPECT, health.state());

        health.watch(4);
        assertEquals(4, health.watchedId());
        assertEquals(CoordinatorHealth.State.ALIVE, health.state());
        assertEquals(0, health.consecutiveMisses());
    }

    @Test
    void aThresholdBelowOneIsRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorHealth(0, SLOW_MS, SILENCE_MS));
    }

    @Test
    void theMonitorFiresOnceOnTheTransitionAndReportsBothStates() {
        // A probe that answers at first, then stops answering entirely.
        AtomicInteger probes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        List<String> events = new ArrayList<>();
        CoordinatorHealthMonitor monitor = new CoordinatorHealthMonitor(1, () -> 3,
                coordinatorId -> probes.incrementAndGet() <= 2
                        ? new CoordinatorHealthMonitor.ProbeResult(5L, null, 3, true)
                        : null,
                coordinatorId -> failures.incrementAndGet(), (term, view) -> { }, events::add,
                3, SLOW_MS, SILENCE_MS);
        try {
            for (int tick = 0; tick < 2; tick++) {
                monitor.tick();
            }
            assertEquals(0, failures.get(), "a responsive coordinator never triggers an election");
            assertFalse(events.stream().anyMatch(line -> line.startsWith(WorkerEvent.COORDINATOR_FAILED)),
                    "no failure event while the coordinator answers");

            // Three misses reach the threshold of three; further misses must not fire again.
            monitor.tick();
            assertEquals(0, failures.get(), "one missed probe is suspicion, not failure");
            monitor.tick();
            monitor.tick();
            assertEquals(1, failures.get(), "the run of misses raises exactly one election");
            monitor.tick();
            monitor.tick();
            assertEquals(1, failures.get(), "a coordinator already known dead stays quiet");

            long failureEvents = events.stream()
                    .filter(line -> line.startsWith(WorkerEvent.COORDINATOR_FAILED)).count();
            assertEquals(1, failureEvents, "the feed gets one COORDINATOR_FAILED line, not one per tick");
            assertTrue(events.stream().anyMatch(line -> line.startsWith(WorkerEvent.COORDINATOR_SUSPECTED)),
                    "the feed must show the suspicion before the failure");
        } finally {
            monitor.close();
        }
    }

    @Test
    void theMonitorIgnoresAWorkerThatLeadsItselfOrHasNoLeader() {
        AtomicInteger probes = new AtomicInteger();
        CoordinatorHealthMonitor selfLeader = new CoordinatorHealthMonitor(3, () -> 3,
                id -> { probes.incrementAndGet(); return null; }, id -> { }, (t, v) -> { },
                message -> { });
        CoordinatorHealthMonitor noLeader = new CoordinatorHealthMonitor(3, () -> -1,
                id -> { probes.incrementAndGet(); return null; }, id -> { }, (t, v) -> { },
                message -> { });
        try {
            selfLeader.tick();
            noLeader.tick();
            assertEquals(0, probes.get(), "neither case probes anything");
        } finally {
            selfLeader.close();
            noLeader.close();
        }
    }

    @Test
    void defaultsDetectADeadCoordinatorInRoughlyThreeToFiveSeconds() {
        // The demo promise, stated as a calculation over the shipped constants.
        long worstCaseMiss = Constants.FAILOVER_HEARTBEAT_INTERVAL_MS
                + Constants.FAILOVER_HEARTBEAT_DEADLINE_MS;
        long detection = worstCaseMiss * Constants.FAILOVER_HEARTBEAT_FAILURE_THRESHOLD;
        assertTrue(detection >= 3_000 && detection <= 6_000,
                "detection took " + detection + "ms, outside the documented window");
    }
}