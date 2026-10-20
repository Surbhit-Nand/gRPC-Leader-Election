package com.cs324.worker;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

// Renew the leader lease from majority reachability; repeated quorum loss forces it to step down.
final class QuorumMonitor implements AutoCloseable {

    private final int selfId;
    private final BooleanSupplier isLeader;
    private final Supplier<Set<Integer>> readyMembers;
    private final IntPredicate reachable;
    private final Runnable onQuorum;
    private final Runnable onLost;
    private final Consumer<String> log;
    private final int lossThreshold;
    private final long intervalMillis;
    private final AtomicInteger consecutiveLosses = new AtomicInteger();
    private final ScheduledExecutorService timer;
    private volatile boolean lostReported;
    private volatile boolean closed;

    QuorumMonitor(int selfId, BooleanSupplier isLeader, Supplier<Set<Integer>> readyMembers,
            IntPredicate reachable, Runnable onQuorum, Runnable onLost, Consumer<String> log,
            int lossThreshold, long intervalMillis) {
        this.selfId = selfId;
        this.isLeader = isLeader;
        this.readyMembers = readyMembers;
        this.reachable = reachable;
        this.onQuorum = onQuorum;
        this.onLost = onLost;
        this.log = log;
        this.lossThreshold = Math.max(1, lossThreshold);
        this.intervalMillis = Math.max(50L, intervalMillis);
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "quorum-monitor-" + selfId);
            thread.setDaemon(true);
            return thread;
        });
    }

    void start() {
        timer.scheduleWithFixedDelay(this::tick, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    // One check, package-visible so tests can step the monitor without waiting.
    void tick() {
        if (closed) {
            return;
        }
        if (!isLeader.getAsBoolean()) {
            consecutiveLosses.set(0);
            lostReported = false;
            return;
        }
        int reachableCount = 1;
        int quorumSize;
        try {
            Set<Integer> members = new HashSet<>(readyMembers.get());
            members.add(selfId);
            quorumSize = members.size() / 2 + 1;
            for (int member : members) {
                if (member != selfId && reachable.test(member)) {
                    reachableCount++;
                }
            }
        } catch (RuntimeException directoryUnavailable) {
            // Treat unavailable membership as lost quorum to prevent serving with an unverified majority.
            log.accept("quorum check could not read membership: " + directoryUnavailable.getMessage());
            reachableCount = 1;
            quorumSize = 2;
        }
        if (reachableCount >= quorumSize) {
            consecutiveLosses.set(0);
            if (lostReported) {
                lostReported = false;
                log.accept("quorum restored for Worker " + selfId + " (" + reachableCount + "/" + quorumSize + ")");
            }
            onQuorum.run();
            return;
        }
        int losses = consecutiveLosses.incrementAndGet();
        if (!lostReported && losses >= lossThreshold) {
            lostReported = true;
            log.accept("Worker " + selfId + " lost its quorum (" + reachableCount + "/" + quorumSize
                    + " reachable); stepping down");
            onLost.run();
        }
    }

    boolean hasLostQuorum() {
        return lostReported;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            timer.shutdownNow();
        }
    }
}
