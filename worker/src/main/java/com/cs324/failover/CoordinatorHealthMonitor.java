package com.cs324.failover;

import com.cs324.common.Constants;
import com.cs324.common.WorkerEvent;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

// Probe the coordinator and trigger recovery only on a transition to failed.
public final class CoordinatorHealthMonitor implements AutoCloseable {

    // One probe to a coordinator; returning null means the probe did not complete.
    public interface Probe {
        // Returns latency plus the responder's view, or null when the probe failed.
        ProbeResult send(int coordinatorId);
    }

    // What one probe learned: how long it took, and what the responder believes.
    public record ProbeResult(long latencyMillis, Long term, int coordinatorIdView, boolean isCoordinator) {}

    private final int selfId;
    private final IntSupplier believedCoordinator;
    private final Probe probe;
    private final IntConsumer onFailed;
    private final Consumer<ProbeResult> onLeadershipDrift;
    private final Consumer<String> log;
    private final CoordinatorHealth health;
    private final ScheduledExecutorService timer;
    private final AtomicBoolean inFlight = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public CoordinatorHealthMonitor(int selfId, IntSupplier believedCoordinator, Probe probe,
            IntConsumer onFailed, Consumer<ProbeResult> onLeadershipDrift, Consumer<String> log) {
        this(selfId, believedCoordinator, probe, onFailed, onLeadershipDrift, log,
                Constants.FAILOVER_HEARTBEAT_FAILURE_THRESHOLD,
                Constants.FAILOVER_HEARTBEAT_SLOW_MS, Constants.FAILOVER_HEARTBEAT_SILENCE_MS);
    }

    public CoordinatorHealthMonitor(int selfId, IntSupplier believedCoordinator, Probe probe,
            IntConsumer onFailed, Consumer<ProbeResult> onLeadershipDrift, Consumer<String> log,
            int failureThreshold, long slowThresholdMillis, long silenceWindowMillis) {
        this.selfId = selfId;
        this.believedCoordinator = believedCoordinator;
        this.probe = probe;
        this.onFailed = onFailed;
        this.onLeadershipDrift = onLeadershipDrift;
        this.log = log;
        this.health = new CoordinatorHealth(failureThreshold, slowThresholdMillis, silenceWindowMillis);
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "coordinator-health-" + selfId);
            thread.setDaemon(true);
            return thread;
        });
    }

    // The last verdict, so the feed and tests can read the machine's own view.
    public CoordinatorHealth.State state() {
        return health.state();
    }

    public void start() {
        timer.scheduleWithFixedDelay(this::tick,
                Constants.FAILOVER_HEARTBEAT_INTERVAL_MS,
                Constants.FAILOVER_HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // One detection cycle. Package-visible so a test can step the machine without waiting for the scheduler.
    void tick() {
        if (closed.get()) {
            return;
        }
        int coordinator = believedCoordinator.getAsInt();
        if (coordinator == -1 || coordinator == selfId) {
            // Nothing to watch: either nobody leads yet, or we lead and answer our own probes.
            return;
        }
        // One probe at a time, so a slow coordinator cannot pile up overlapping calls.
        if (!inFlight.compareAndSet(false, true)) {
            return;
        }
        try {
            long now = System.currentTimeMillis();
            health.watch(coordinator);
            CoordinatorHealth.State previous = health.state();
            long started = System.nanoTime();
            ProbeResult result;
            try {
                result = probe.send(coordinator);
            } catch (RuntimeException e) {
                log.accept("heartbeat probe threw: " + e.getMessage());
                result = null;
            }
            long latencyMillis = (System.nanoTime() - started) / 1_000_000L;
            CoordinatorHealth.State state = result == null
                    ? health.recordMiss(coordinator, now)
                    : health.recordSuccess(coordinator, Math.max(result.latencyMillis(), 0L), now);
            report(previous, state, coordinator, latencyMillis);
            if (result != null) {
                observeLeadership(coordinator, result);
            }
            if (health.justFailed(previous)) {
                log.accept(WorkerEvent.COORDINATOR_FAILED + ": Worker " + coordinator
                        + " failed after " + health.consecutiveMisses() + " missed probes; electing a replacement");
                onFailed.accept(coordinator);
            }
        } finally {
            inFlight.set(false);
        }
    }

    // Keeps the feed readable: only transitions are worth a line, not every probe.
    private void report(CoordinatorHealth.State previous, CoordinatorHealth.State state,
            int coordinator, long latencyMillis) {
        if (state == previous) {
            return;
        }
        switch (state) {
            case SUSPECT -> {
                if (previous == CoordinatorHealth.State.ALIVE) {
                    log.accept(WorkerEvent.COORDINATOR_SUSPECTED + ": Worker " + coordinator
                            + " has not answered normally (last probe " + latencyMillis + "ms, "
                            + health.consecutiveMisses() + " miss(es)); still counted as active");
                } else {
                    log.accept(WorkerEvent.COORDINATOR_SUSPECTED + ": Worker " + coordinator
                            + " is answering slowly (" + latencyMillis + "ms); still counted as active");
                }
            }
            case ALIVE -> log.accept("coordinator Worker " + coordinator + " is answering normally again");
            case FAILED -> { }
        }
    }

    // Adopt a newer reported leadership view instead of starting an unnecessary election.
    private void observeLeadership(int coordinator, ProbeResult result) {
        if (result.term() != null && result.term() > 0) {
            onLeadershipDrift.accept(result);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            timer.shutdownNow();
        }
    }
}
