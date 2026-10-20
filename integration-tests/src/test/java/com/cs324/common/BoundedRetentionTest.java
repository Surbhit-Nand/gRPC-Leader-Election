package com.cs324.common;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedRetentionTest {

    @Test
    void mapEvictsTheLeastRecentlyUsedEntry() {
        BoundedMap<Integer, String> map = new BoundedMap<>(2);
        assertNull(map.putIfAbsent(1, "one"));
        assertEquals("one", map.get(1));
        map.put(2, "two");
        // Touch 1 so 2 is now the oldest entry.
        assertEquals("one", map.get(1));
        map.put(3, "three");

        assertTrue(map.containsKey(1));
        assertFalse(map.containsKey(2));
        assertTrue(map.containsKey(3));
        assertEquals(2, map.size());
    }

    @Test
    void mapSupportsComputeAndConditionalRemove() {
        BoundedMap<String, AtomicInteger> map = new BoundedMap<>(4);
        map.compute("counter", (key, existing) -> existing == null ? new AtomicInteger(1) : null);
        assertEquals(1, map.get("counter").get());
        map.compute("counter", (key, existing) -> new AtomicInteger(existing.get() + 1));
        assertEquals(2, map.get("counter").get());
        assertTrue(map.remove("counter", map.get("counter")));
        assertFalse(map.containsKey("counter"));
    }

    @Test
    void setDropsTheOldestValuePastItsBound() {
        BoundedSet<Long> set = new BoundedSet<>(2);
        assertTrue(set.add(1L));
        assertTrue(set.add(2L));
        assertTrue(set.add(3L));

        assertFalse(set.contains(1L));
        assertTrue(set.contains(2L));
        assertTrue(set.contains(3L));
        assertEquals(2, set.size());
    }
}
