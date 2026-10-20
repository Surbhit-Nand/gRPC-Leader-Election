package com.cs324.failover;

// Track probe latency and silence on the monitor thread; only repeated misses or prolonged silence fail.
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

    // Points the monitor at a (possibly new) coordinator, forgetting the previous verdict.
    public void watch(int coordinatorId) {
        if (coordinatorId != watchedId) {
            watchedId = coordinatorId;
            reset();
        }
    }

    // Clear missed probes on any reply; a slow reply marks the coordinator suspect, not failed.
    public State recordSuccess(int coordinatorId, long latencyMillis, long nowMillis) {
        watch(coordinatorId);
        consecutiveMisses = 0;
        lastSuccessMillis = nowMillis;
        sawSuccess = true;
        state = latencyMillis > slowThresholdMillis ? State.SUSPECT : State.ALIVE;
        return state;
    }

    // Record a missed probe and return the updated health state.
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

    // Declare failure when the silence window expires, even without enough consecutive misses.
    public State evaluate(long nowMillis) {
        if (state == State.FAILED) {
            return state;
        }
        if (sawSuccess && nowMillis - lastSuccessMillis > silenceWindowMillis) {
            state = State.FAILED;
        }
        return state;
    }

    // True on the transition into FAILED, which is the only moment an election may start.
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