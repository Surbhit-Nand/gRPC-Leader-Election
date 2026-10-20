package com.cs324.failover;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Decides whether a detected coordinator failure is new, so one death causes one election.
 *
 * <p>Several workers notice the same dead coordinator within a few hundred milliseconds of each
 * other. Each of them noticing is correct; each of them waving is not, so the first detection
 * wins and the rest wait out the window.
 *
 * <p>The key is (coordinator, when) rather than the coordinator alone. Keying on the id alone
 * would suppress a *second, unrelated* death of the same worker id for ever, and the cluster
 * would then sit with a dead coordinator and no election to replace it.
 *
 * <p>The clock is injected so the window can be tested without sleeping.
 */
public final class FailureGate {

    private record Failure(int coordinatorId, long atMillis) {}

    private final AtomicReference<Failure> last = new AtomicReference<>();
    private final long windowMillis;
    private final LongSupplier clock;

    public FailureGate(long windowMillis, LongSupplier clock) {
        if (windowMillis < 0) {
            throw new IllegalArgumentException("The suppression window cannot be negative");
        }
        this.windowMillis = windowMillis;
        this.clock = clock;
    }

    /**
     * Claims the right to elect for one failure.
     *
     * @return true exactly once per failure: the first detection, and then again only after the
     *         window has passed or a different coordinator has failed.
     */
    public boolean shouldElect(int coordinatorId) {
        long now = clock.getAsLong();
        Failure previous = last.get();
        if (previous != null && previous.coordinatorId() == coordinatorId
                && now - previous.atMillis() < windowMillis) {
            return false;
        }
        last.set(new Failure(coordinatorId, now));
        return true;
    }
}