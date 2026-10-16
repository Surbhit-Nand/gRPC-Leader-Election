package com.cs324.computation;

import com.cs324.computation.proto.*;
import com.cs324.worker.WorkerTaskCalculator;

/** Typed boundary around the existing, independently tested calculation helpers. */
final class ComputationCodec {
    private ComputationCodec() {}

    static Object decode(JobType type, WorkInput input) {
        if (type == JobType.JOB_TYPE_UNSPECIFIED || type == JobType.UNRECOGNIZED)
            throw new IllegalArgumentException("A supported job type is required");
        return switch (input.getInputCase()) {
            case NUMBERS -> {
                if (type == JobType.PRIMESUM)
                    throw new IllegalArgumentException("PRIMESUM requires an inclusive range");
                if (type == JobType.MAX && input.getNumbers().getValuesCount() == 0)
                    throw new IllegalArgumentException("MAX requires at least one number");
                yield input.getNumbers().getValuesList().stream().mapToInt(Integer::intValue).toArray();
            }
            case RANGE -> {
                if (type != JobType.PRIMESUM)
                    throw new IllegalArgumentException("Only PRIMESUM accepts a range");
                yield new WorkerTaskCalculator.PrimeRange(input.getRange().getStart(), input.getRange().getEnd());
            }
            case INPUT_NOT_SET -> throw new IllegalArgumentException("Job input is required");
        };
    }

    static WorkInput encode(Object chunk) {
        if (chunk instanceof WorkerTaskCalculator.PrimeRange range)
            return WorkInput.newBuilder().setRange(PrimeRange.newBuilder()
                    .setStart(range.start()).setEnd(range.end())).build();
        Numbers.Builder numbers = Numbers.newBuilder();
        for (int value : (int[]) chunk) numbers.addValues(value);
        return WorkInput.newBuilder().setNumbers(numbers).build();
    }

    static long units(Object chunk) {
        return chunk instanceof WorkerTaskCalculator.PrimeRange range
                ? (long) range.end() - range.start() + 1 : ((int[]) chunk).length;
    }
}
