package com.cs324.worker;

import com.cs324.worker.WorkerTaskCalculator.PrimeRange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Check large-range splitting by endpoints and balance without computing the prime results.
class WorkerTaskDistributorScaleTest {

    @Test
    void splitsALargeRangeAcrossManyWorkersWithoutGaps() {
        PrimeRange range = new PrimeRange(1, 10_000_000);
        int workers = 64;

        List<Object> chunks = WorkerTaskDistributor.split("PRIMECOUNT", range, workers);

        assertEquals(workers, chunks.size());
        long cursor = range.start();
        long totalWidth = 0;
        long widest = 0;
        long narrowest = Long.MAX_VALUE;
        for (Object chunk : chunks) {
            PrimeRange part = (PrimeRange) chunk;
            assertEquals(cursor, part.start(), "chunks must be contiguous");
            long width = (long) part.end() - part.start() + 1;
            assertTrue(width > 0, "no worker should receive an empty range");
            totalWidth += width;
            widest = Math.max(widest, width);
            narrowest = Math.min(narrowest, width);
            cursor = (long) part.end() + 1;
        }

        assertEquals(10_000_000L, totalWidth);
        assertEquals(10_000_001L, cursor);
        assertTrue(widest - narrowest <= 1, "range slices should differ by at most one value");
    }
}
