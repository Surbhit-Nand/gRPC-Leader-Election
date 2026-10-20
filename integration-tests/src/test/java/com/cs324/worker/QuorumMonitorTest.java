package com.cs324.worker;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuorumMonitorTest {

    @Test
    void leaderStepsDownOnlyAfterTheConfiguredNumberOfLostChecks() {
        AtomicBoolean leader = new AtomicBoolean(true);
        AtomicInteger quorumChecks = new AtomicInteger();
        AtomicInteger stepDowns = new AtomicInteger();
        QuorumMonitor monitor = monitor(leader, () -> Set.of(1, 2, 3, 4), member -> member == 3,
                quorumChecks, stepDowns, 2);

        monitor.tick();
        assertEquals(0, stepDowns.get(), "one slow check is tolerated");
        assertFalse(monitor.hasLostQuorum());
        monitor.tick();
        assertEquals(1, stepDowns.get(), "the second consecutive miss removes the leader");
        assertTrue(monitor.hasLostQuorum());
        assertEquals(0, quorumChecks.get());
        monitor.close();
    }

    @Test
    void aMajorityKeepsTheLeaseAndARecoveredLeaderCanBeAcknowledgedAgain() {
        AtomicBoolean leader = new AtomicBoolean(true);
        AtomicInteger quorumChecks = new AtomicInteger();
        AtomicInteger stepDowns = new AtomicInteger();
        AtomicBoolean thirdReachable = new AtomicBoolean(true);
        QuorumMonitor monitor = monitor(leader, () -> Set.of(1, 2, 3, 4),
                member -> member == 2 || (member == 3 && thirdReachable.get()),
                quorumChecks, stepDowns, 1);

        monitor.tick();
        assertEquals(1, quorumChecks.get(), "self, Worker 2 and Worker 3 are a majority");
        assertEquals(0, stepDowns.get());
        thirdReachable.set(false);
        monitor.tick();
        assertEquals(1, stepDowns.get(), "self plus Worker 2 is only half of four");
        leader.set(false);
        monitor.tick();
        assertFalse(monitor.hasLostQuorum(), "a follower does not carry a lost-leader latch");

        thirdReachable.set(true);
        leader.set(true);
        monitor.tick();
        assertEquals(2, quorumChecks.get(), "a recovered majority renews the lease");
        monitor.close();
    }

    @Test
    void unreadableMembershipFailsClosed() {
        AtomicBoolean leader = new AtomicBoolean(true);
        AtomicInteger stepDowns = new AtomicInteger();
        Supplier<Set<Integer>> unavailable = () -> {
            throw new IllegalStateException("bootstrap unavailable");
        };
        QuorumMonitor monitor = monitor(leader, unavailable, member -> true,
                new AtomicInteger(), stepDowns, 1);

        monitor.tick();
        assertEquals(1, stepDowns.get());
        monitor.close();
    }

    private QuorumMonitor monitor(AtomicBoolean leader, Supplier<Set<Integer>> members,
            java.util.function.IntPredicate reachable, AtomicInteger quorumChecks,
            AtomicInteger stepDowns, int threshold) {
        return new QuorumMonitor(4, leader::get, members, reachable,
                quorumChecks::incrementAndGet, () -> {
                    stepDowns.incrementAndGet();
                    leader.set(false);
                }, ignored -> { }, threshold, 100_000);
    }
}
