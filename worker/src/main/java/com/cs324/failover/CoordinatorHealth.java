package com.cs324.failover;

/**
 * The liveness rule for one watched coordinator, with no I/O in it, so the thresholds can be
 * reasoned about and unit-tested on their own.
 *
 * <p>The distinction the class exists to make: a coordinator that is <em>slow</em> is not a
 * coordinator that is <em>gone</em>. A slow one still answers, so it proves it is alive and
 * must never cost the cluster an election; only repeated silence, or silence for long enough
 * that an earlier success cannot vouch for it, counts as failure.
 *
 * <pre>
 *   ALIVE    - last probe answered inside the slow threshold
 *   SUSPECT  - answered but slowly, or one probe missed
 *   FAILED   - {@code failureThreshold} consecutive misses, or no success within the silence window
 * </pre>
 *
 * <p>Instances are confined to the monitor thread.
 */
public final class CoordinatorHealth {

    public enum State { ALIVE, SUSPECT, FAILED }

    private final int failureThreshold;
    private final long slowThresholdMillis;
    private final long silenceWindowMillis;

    private State state = State.ALIVE;
    private int watchedId = -1;
    private int consecutiveMisses;
    private long lastSuccessMillis;
    private boolean sawSuccess;

    public CoordinatorHealth(int failureThreshold, long slowThresholdMillis, long silenceWindowMillis) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("At least one miss is needed to declare failure");
        }
        this.failureThreshold = failureThreshold;
        this.slowThresholdMillis = slowThresholdMillis;
        this.silenceWindowMillis = silenceWindowMillis;
    }

    public State state() {
        return state;
    }

    public int watchedId() {
        return watchedId;
    }

    public int consecutiveMisses() {
        return consecutiveMisses;
    }

    /** Points the monitor at a (possibly new) coordinator, forgetting the previous verdict. */
    public void watch(int coordinatorId) {
        if (coordinatorId != watchedId) {
            watchedId = coordinatorId;
            reset();
        }
    }

    /**
     * Records an answered probe.
     *
     * <p>Any answer clears the miss streak, because the coordinator proved it is reachable. A
     * slow answer moves it to {@code SUSPECT} rather than {@code ALIVE}, which is what lets a
     * recovering-but-struggling coordinator be visible in the feed without triggering anything.
     *
     * @return the state after the update.
     */
    public State recordSuccess(int coordinatorId, long latencyMillis, long nowMillis) {
        watch(coordinatorId);
        consecutiveMisses = 0;
        lastSuccessMillis = nowMillis;
        sawSuccess = true;
        state = latencyMillis > slowThresholdMillis ? State.SUSPECT : State.ALIVE;
        return state;
    }

    /**
     * Records a probe that did not complete: a timeout, an unreachable endpoint, or a
     * coordinator this worker cannot even resolve.
     *
     * @return the state after the update.
     */
    public State recordMiss(int coordinatorId, long nowMillis) {
        watch(coordinatorId);
        consecutiveMisses++;
        if (consecutiveMisses >= failureThreshold) {
            state = State.FAILED;
        } else if (state != State.FAILED) {
            state = State.SUSPECT;
        }
        return evaluate(nowMillis);
    }

    /**
     * Applies the silence window: a coordinator that has not answered for longer than the
     * window is failed regardless of the streak, because an old success cannot vouch for a
     * peer that has since gone quiet.
     *
     * @return the state after the check.
     */
    public State evaluate(long nowMillis) {
        if (state == State.FAILED) {
            return state;
        }
        if (sawSuccess && nowMillis - lastSuccessMillis > silenceWindowMillis) {
            state = State.FAILED;
        }
        return state;
    }

    /** True on the transition into FAILED, which is the only moment an election may start. */
    public boolean justFailed(State previous) {
        return previous != State.FAILED && state == State.FAILED;
    }

    private void reset() {
        state = State.ALIVE;
        consecutiveMisses = 0;
        lastSuccessMillis = 0L;
        sawSuccess = false;
    }
}