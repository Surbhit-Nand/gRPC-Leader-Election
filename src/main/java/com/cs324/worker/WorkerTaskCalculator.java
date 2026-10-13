package com.cs324.worker;

import java.util.Arrays;
import java.util.Collection;
import java.io.Serializable;
import java.util.stream.IntStream;

// Shared worker calculations, with payloads given as lists or an explicit range.
public final class WorkerTaskCalculator {
    private WorkerTaskCalculator() {}

    // Inclusive prime range, serializable so it can cross the RMI wire.
    public record PrimeRange(int start, int end) implements Serializable {
        public PrimeRange {
            // A reversed range can never be split or scanned, so reject it early.
            if (start > end) {
                throw new IllegalArgumentException("Range start must not exceed end.");
            }
        }
    }

    // Entry point for all three job types; the type is matched case-insensitively.
    // Bad payloads and an empty MAX list throw; empty prime lists return zero.
    public static Number calculate(String jobType, Object payload) {
        // Prime sums accumulate as long, because the total can exceed one integer.
        if ("MAX".equalsIgnoreCase(jobType)) {
            return numbers(jobType, payload).max()
                // An empty payload has no maximum, so it is an error, not a default.
                    .orElseThrow(() -> new IllegalArgumentException("MAX requires at least one value."));
        }
        if ("PRIMESUM".equalsIgnoreCase(jobType)) {
            return numbers(jobType, payload).filter(WorkerTaskCalculator::isPrime)
                // addExact surfaces overflow instead of returning a wrong sum.
                    .asLongStream().reduce(0L, Math::addExact);
        }
        if ("PRIMECOUNT".equalsIgnoreCase(jobType)) {
            return Math.toIntExact(numbers(jobType, payload)
                // toIntExact rejects a count that will not fit the int RMI contract.
                    .filter(WorkerTaskCalculator::isPrime).count());
        }
        // An unsupported type is a programming error, not a runtime condition.
        throw new IllegalArgumentException("Unsupported job type: " + jobType);
    }

    private static IntStream numbers(String jobType, Object payload) {
        // A PrimeRange is scanned directly, so no list is ever materialised for it.
        if (payload instanceof PrimeRange range) {
            if ("MAX".equalsIgnoreCase(jobType)) {
                throw new IllegalArgumentException("MAX requires a list, not a prime range.");
            }
            // Negatives and 0/1 cannot be prime, so the scan starts at 2.
            return IntStream.rangeClosed(Math.max(2, range.start()), range.end());
        }
        // int[], Integer[] and Collection payloads are all read as lists, duplicates kept.
        if (payload instanceof int[] values) {
            return Arrays.stream(values);
        }
        if (payload instanceof Integer[] values) {
            // Reject non-Integer entries rather than silently coercing them.
            return Arrays.stream(values).mapToInt(WorkerTaskCalculator::requireInteger);
        }
        if (payload instanceof Collection<?> values) {
            return values.stream().mapToInt(WorkerTaskCalculator::requireInteger);
        }
        throw new IllegalArgumentException("Payload must be int[], Integer[], a collection of Integer, or PrimeRange.");
    }

    private static int requireInteger(Object value) {
        if (value instanceof Integer integer) {
            return integer;
        }
        // Fail loudly: a null or fractional entry would become a wrong answer.
        throw new IllegalArgumentException("List entries must be non-null Integer values.");
    }

    // Exact primality test, using division so no squaring overflow is possible.
    public static boolean isPrime(int value) {
        if (value < 2) {
            return false;
        }
        // 2 is the only even prime, so settle it here and test odd divisors below.
        if (value % 2 == 0) {
            return value == 2;
        }
        // divisor <= value / divisor stops at the square root with no overflow.
        for (int divisor = 3; divisor <= value / divisor; divisor += 2) {
            if (value % divisor == 0) {
                return false;
            }
        }
        return true;
    }
}
