package com.cs324.failover;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

// Suppress repeated local election requests for a coordinator failure within a time window.
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

    // Allow a failure election when its suppression window has expired or the failed coordinator changes.
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