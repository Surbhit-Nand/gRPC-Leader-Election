package com.cs324.worker;

import java.util.Arrays;
import java.util.List;
import com.cs324.worker.WorkerTaskCalculator.PrimeRange;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

// WorkerTaskCalculator.calculate(): which payloads are valid, and how ranges bound.
class WorkerTaskCalculatorTest {
    // Ranges may straddle zero, so the prime scan cannot assume a positive lower bound.
    @Test
    void smallPrimesAndRangesCrossingZeroAreCalculatedCorrectly() {
        for (int value : new int[]{2, 3, 5, 7, 97}) {
            assertTrue(WorkerTaskCalculator.isPrime(value), "Expected prime: " + value);
        }
        assertEquals(Long.valueOf(5), WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(Integer.MIN_VALUE, 3)));
        assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", new PrimeRange(-5, 3)));
        assertEquals(Long.valueOf(0), WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(-5, 1)));
        assertEquals(Integer.valueOf(0), WorkerTaskCalculator.calculate("PRIMECOUNT", new PrimeRange(8, 10)));
    }

    @Test
    // A two-element list is data, however much it happens to look like a range.
    void twoElementListChunksNeverBecomeRanges() {
        int[] first = {2, 7};
        int[] second = {5, 5};
        int[] whole = {2, 7, 5, 5};
        assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", first));
        assertEquals(Long.valueOf(9), WorkerTaskCalculator.calculate("PRIMESUM", first));
        assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", second));
        // Split and recombine must agree: the whole equals the sum of its parts.
        for (String operation : new String[]{"PRIMESUM", "PRIMECOUNT"}) {
            assertEquals(WorkerTaskCalculator.calculate(operation, whole).longValue(),
                    WorkerTaskCalculator.calculate(operation, first).longValue()
                            + WorkerTaskCalculator.calculate(operation, second).longValue());
        }
        // MAX is associative, so the same identity must hold for it.
        assertEquals(WorkerTaskCalculator.calculate("MAX", whole).intValue(),
                Math.max(WorkerTaskCalculator.calculate("MAX", first).intValue(),
                        WorkerTaskCalculator.calculate("MAX", second).intValue()));
    }

    @Test
    // Two adjacent inclusive ranges must cover the whole exactly once, no overlap.
    void separateRangesCombineToTheWholeRange() {
        for (String operation : new String[]{"PRIMESUM", "PRIMECOUNT"}) {
            assertEquals(WorkerTaskCalculator.calculate(operation, new PrimeRange(1, 100)).longValue(),
                    WorkerTaskCalculator.calculate(operation, new PrimeRange(1, 50)).longValue()
                            + WorkerTaskCalculator.calculate(operation, new PrimeRange(51, 100)).longValue());
        }
    }

    @Test
    // MAX must handle negatives and a single-element list, with no zero seed.
    void maxUsesIntegerListsIncludingMinimumValue() {
        assertEquals(Integer.valueOf(45), WorkerTaskCalculator.calculate("max", new int[]{3, 7, 12, 45}));
        assertEquals(Integer.valueOf(5), WorkerTaskCalculator.calculate("MAX", new int[]{5, 3}));
        assertEquals(Integer.valueOf(Integer.MIN_VALUE),
                WorkerTaskCalculator.calculate("MAX", new int[]{Integer.MIN_VALUE}));
        assertEquals(Integer.valueOf(-3), WorkerTaskCalculator.calculate("MAX", new int[]{-10, -3, -9}));
    }

    @Test
    // Operation names are case-insensitive, so client casing cannot change the answer.
    void explicitRangesIncludeBothEndpoints() {
        assertEquals(Long.valueOf(17), WorkerTaskCalculator.calculate("primesum", new PrimeRange(1, 10)));
        assertEquals(Integer.valueOf(4), WorkerTaskCalculator.calculate("primecount", new PrimeRange(2, 7)));
        // A one-element range still counts: both endpoints are inclusive.
        assertEquals(Long.valueOf(7), WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(7, 7)));
        assertEquals(Integer.valueOf(1), WorkerTaskCalculator.calculate("PRIMECOUNT", new PrimeRange(7, 7)));
        // Inverted bounds, and a range handed to MAX, are rejected rather than guessed.
        assertThrows(IllegalArgumentException.class, () -> new PrimeRange(10, 1));
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate("MAX", new PrimeRange(1, 10)));
    }

    @Test
    // int[], Integer[] and List must all behave identically, duplicates included.
    void allListTypesAgreeAndRetainDuplicates() {
        for (Object payload : new Object[]{new int[]{2, 7}, new Integer[]{2, 7}, List.of(2, 7)}) {
            assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", payload));
            assertEquals(Long.valueOf(9), WorkerTaskCalculator.calculate("PRIMESUM", payload));
        }
        assertEquals(Integer.valueOf(4), WorkerTaskCalculator.calculate("PRIMECOUNT", new int[]{2, 3, 4, 5, 5}));
        assertEquals(Long.valueOf(15), WorkerTaskCalculator.calculate("PRIMESUM", new int[]{2, 3, 5, 5}));
    }

    @Test
    // An empty list has no maximum, but the prime operations still total zero.
    void emptyListsHaveNoMaximumButZeroPrimeSumAndCount() {
        for (Object payload : new Object[]{new int[0], new Integer[0], List.of()}) {
            assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate("MAX", payload));
            assertEquals(Integer.valueOf(0), WorkerTaskCalculator.calculate("PRIMECOUNT", payload));
            assertEquals(Long.valueOf(0), WorkerTaskCalculator.calculate("PRIMESUM", payload));
        }
    }

    @Test
    // Nothing is coerced: bad shapes, nulls, floats and oversized longs are errors.
    void rejectsInvalidPayloadsAndUnsafeNumberConversions() {
        for (String operation : new String[]{"MAX", "PRIMESUM", "PRIMECOUNT"}) {
            for (Object payload : new Object[]{null, "invalid", new long[]{2, 7},
                    new Integer[]{2, null}, Arrays.asList(2, null), List.of(2, "skip"),
                    List.of(5.9), List.of(2L), List.of(Long.MAX_VALUE)}) {
                assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate(operation, payload));
            }
        }
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate("SUM", new int[]{1}));
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate(null, new int[]{1}));
    }

    @Test
    // The integer extremes must not overflow trial division or the range arithmetic.
    void primeChecksAndRangeEndpointsHandleIntegerLimits() {
        assertTrue(WorkerTaskCalculator.isPrime(Integer.MAX_VALUE));
        // 561 is a Carmichael number: it fools naive divisibility tests but is composite.
        for (int value : new int[]{Integer.MIN_VALUE, -2, 0, 1, 4, 9, 121, 561, 2147395600}) {
            assertFalse(WorkerTaskCalculator.isPrime(value));
        }
        assertEquals(Long.valueOf(Integer.MAX_VALUE), WorkerTaskCalculator.calculate(
                "PRIMESUM", new PrimeRange(Integer.MAX_VALUE - 1, Integer.MAX_VALUE)));
        assertEquals(Integer.valueOf(1), WorkerTaskCalculator.calculate(
                "PRIMECOUNT", new PrimeRange(Integer.MAX_VALUE, Integer.MAX_VALUE)));
        // The sum has to widen to long, since 1..1,000,000 overflows int on its own.
        assertEquals(Long.valueOf(37_550_402_023L), WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(1, 1_000_000)));
    }
}
