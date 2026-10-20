package com.cs324.common;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

// Thread-safe bounded map; accessing an entry refreshes its eviction order.
public final class BoundedMap<K, V> {

    private final int maxSize;
    private final LinkedHashMap<K, V> entries;

    public BoundedMap(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("A bounded map needs a positive capacity");
        }
        this.maxSize = maxSize;
        this.entries = new LinkedHashMap<>(Math.min(maxSize, 1024), 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > BoundedMap.this.maxSize;
            }
        };
    }

    public synchronized V get(Object key) {
        return entries.get(key);
    }

    public synchronized V put(K key, V value) {
        return entries.put(key, value);
    }

    public synchronized V putIfAbsent(K key, V value) {
        V previous = entries.get(key);
        if (previous == null) {
            entries.put(key, value);
        }
        return previous;
    }

    public synchronized V remove(Object key) {
        return entries.remove(key);
    }

    public synchronized boolean remove(Object key, Object value) {
        V current = entries.get(key);
        if (current != null && current.equals(value)) {
            entries.remove(key);
            return true;
        }
        return false;
    }

    public synchronized boolean containsKey(Object key) {
        return entries.containsKey(key);
    }

    public synchronized V compute(K key, BiFunction<? super K, ? super V, ? extends V> remapping) {
        V previous = entries.get(key);
        V next = remapping.apply(key, previous);
        if (next == null) {
            entries.remove(key);
        } else {
            entries.put(key, next);
        }
        return next;
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized Map<K, V> snapshot() {
        return Map.copyOf(entries);
    }
}
