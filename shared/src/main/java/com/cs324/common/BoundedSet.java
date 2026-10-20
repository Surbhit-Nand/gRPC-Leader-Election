package com.cs324.common;

import java.util.LinkedHashSet;

// Thread-safe bounded set that discards the oldest election message IDs.
public final class BoundedSet<E> {

    private final int maxSize;
    private final LinkedHashSet<E> entries = new LinkedHashSet<>();

    public BoundedSet(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("A bounded set needs a positive capacity");
        }
        this.maxSize = maxSize;
    }

    public synchronized boolean add(E value) {
        if (!entries.add(value)) {
            return false;
        }
        while (entries.size() > maxSize) {
            E oldest = entries.iterator().next();
            entries.remove(oldest);
        }
        return true;
    }

    public synchronized boolean contains(Object value) {
        return entries.contains(value);
    }

    public synchronized int size() {
        return entries.size();
    }
}
