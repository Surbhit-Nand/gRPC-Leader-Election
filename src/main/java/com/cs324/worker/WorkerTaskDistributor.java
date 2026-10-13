package com.cs324.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import com.cs324.worker.WorkerTaskCalculator.PrimeRange;

// Splits a job into balanced chunks and combines the answers the workers return.
public final class WorkerTaskDistributor {
    // Static helper only; there is no state to instance.
    private WorkerTaskDistributor() {}

    public static List<Object> split(String jobType, Object payload, int workers) {
        // A split into zero pieces would lose the job, so require at least one.
        if (workers < 1) throw new IllegalArgumentException("At least one worker is required.");
        // Unknown types are rejected here too, so a bad job never reaches the workers.
        if (!"MAX".equalsIgnoreCase(jobType) && !"PRIMESUM".equalsIgnoreCase(jobType)
                && !"PRIMECOUNT".equalsIgnoreCase(jobType)) {
            throw new IllegalArgumentException("Unsupported job type: " + jobType);
        }
        List<Object> chunks = new ArrayList<>();
        // Long arithmetic keeps ranges near the integer limits from overflowing.
        if (payload instanceof PrimeRange range) {
            // A range has no maximum, so MAX cannot be served from one.
            if ("MAX".equalsIgnoreCase(jobType)) {
                throw new IllegalArgumentException("MAX requires a list, not a prime range.");
            }
            long length = (long) range.end() - range.start() + 1;
            // Never emit more chunks than values, or a chunk would come out empty.
            int count = (int) Math.min(workers, length);
            long start = range.start();
            for (int i = 0; i < count; i++) {
                // Remainder values go to the leading chunks, so sizes differ by one at most.
                long size = length / count + (i < length % count ? 1 : 0);
                // Consecutive inclusive ranges, so nothing is processed twice or skipped.
                chunks.add(new PrimeRange((int) start, (int) (start + size - 1)));
                start += size;
            }
            return chunks;
        }
        // Copy first, so splitting never mutates what the caller passed in.
        int[] values;
        if (payload instanceof int[] input) {
            // clone rather than alias: the caller may reuse its array after submitJob returns.
            values = input.clone();
        } else {
            Collection<?> input;
            if (payload instanceof Integer[] array) input = Arrays.asList(array);
            else if (payload instanceof Collection<?> collection) input = collection;
            else throw new IllegalArgumentException("Expected an integer list or PrimeRange.");
            values = input.stream().mapToInt(value -> {
                if (value instanceof Integer integer) return integer;
                throw new IllegalArgumentException("List entries must be non-null Integer values.");
            }).toArray();
        }
        // An empty list is legal for the prime jobs and keeps the job local.
        if (values.length == 0) {
                // An empty list has no maximum, so MAX is rejected.
            if ("MAX".equalsIgnoreCase(jobType)) {
                throw new IllegalArgumentException("MAX requires at least one value.");
            }
            chunks.add(values);
            return chunks;
        }
        // Spread values evenly, giving the remainder to the leading chunks.
        // Fewer chunks than values keeps every dispatched worker actually busy.
        int count = Math.min(workers, values.length);
        int start = 0;
        for (int i = 0; i < count; i++) {
            int size = values.length / count + (i < values.length % count ? 1 : 0);
            chunks.add(Arrays.copyOfRange(values, start, start + size));
            start += size;
        }
        return chunks;
    }

    // MAX takes the largest partial; the prime jobs add theirs.
    public static Number combine(String jobType, Collection<Number> results) {
        if ("MAX".equalsIgnoreCase(jobType)) {
            return results.stream().mapToInt(Number::intValue).max().orElseThrow();
        }
        long sum = 0;
            // addExact reports overflow instead of returning a wrong total.
        for (Number result : results) sum = Math.addExact(sum, result.longValue());
        if ("PRIMESUM".equalsIgnoreCase(jobType)) return Long.valueOf(sum);
            // toIntExact keeps the count inside the int the RMI contract declares.
        if ("PRIMECOUNT".equalsIgnoreCase(jobType)) return Integer.valueOf(Math.toIntExact(sum));
        throw new IllegalArgumentException("Unsupported job type: " + jobType);
    }
}
