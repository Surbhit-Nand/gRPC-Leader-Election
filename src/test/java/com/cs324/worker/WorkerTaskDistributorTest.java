package com.cs324.worker;

import com.cs324.worker.WorkerTaskCalculator.PrimeRange;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

// Split/combine contract: every partition must reproduce the whole answer exactly.
class WorkerTaskDistributorTest {
    // Across three operations, and worker counts that exceed the input, no chunk is empty.
    @Test void partitionsPreserveResultsAndDoNotCreateEmptyMaxChunks() {
        for (String type : List.of("MAX", "PRIMESUM", "PRIMECOUNT")) {
            for (int workers : new int[]{1, 2, 3, 20}) {
                int[] input = {Integer.MIN_VALUE, 2, 7, 5, 5, 11, -3};
                List<Object> chunks = WorkerTaskDistributor.split(type, input, workers);
                assertEquals(Math.min(workers, input.length), chunks.size());
                // The defining property: recomputing from the chunks equals the whole.
                assertEquals(WorkerTaskCalculator.calculate(type, input),
                        WorkerTaskDistributor.combine(type, chunks.stream()
                                .map(chunk -> WorkerTaskCalculator.calculate(type, chunk)).toList()));
            }
        }
    }

    // Ranges split on inclusive boundaries, are sized by width, and clamp at int edges.
    @Test void rangesAreBalancedInclusiveAndSafeAtIntegerLimits() {
        List<Object> chunks = WorkerTaskDistributor.split("PRIMESUM", new PrimeRange(1, 10), 3);
        assertEquals(List.of(new PrimeRange(1, 4), new PrimeRange(5, 7), new PrimeRange(8, 10)), chunks);
        assertEquals(List.of(new PrimeRange(Integer.MIN_VALUE, -1), new PrimeRange(0, Integer.MAX_VALUE)),
                WorkerTaskDistributor.split("PRIMECOUNT", new PrimeRange(Integer.MIN_VALUE, Integer.MAX_VALUE), 2));
        // Ten workers over a one-value range still yields exactly one usable chunk.
        assertEquals(List.of(new PrimeRange(Integer.MAX_VALUE, Integer.MAX_VALUE)),
                WorkerTaskDistributor.split("PRIMESUM", new PrimeRange(Integer.MAX_VALUE, Integer.MAX_VALUE), 10));
    }

    // Bad input is rejected up front rather than yielding a chunk that fails later.
    @Test void validatesBeforeDispatchAndCopiesLists() {
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskDistributor.split("MAX", new int[0], 2));
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskDistributor.split("MAX", List.of(2L), 2));
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskDistributor.split("MAX", new PrimeRange(1, 2), 2));
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskDistributor.split("BAD", new int[]{1}, 2));
        int[] input = {2, 7};
        List<Object> chunks = WorkerTaskDistributor.split("PRIMECOUNT", input, 1);
        input[0] = 0;
        assertArrayEquals(new int[]{2, 7}, (int[]) chunks.getFirst());
        // An empty input is fine for the additive operations; it just gets one chunk.
        assertEquals(1, WorkerTaskDistributor.split("PRIMESUM", new int[0], 3).size());
    }
}
