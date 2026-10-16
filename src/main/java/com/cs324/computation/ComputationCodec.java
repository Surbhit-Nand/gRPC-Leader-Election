package com.cs324.computation;

import com.cs324.job.proto.*;
import com.cs324.worker.WorkerTaskCalculator;
import java.util.Collection;
import java.util.Locale;

/** The shared job.proto is the only computation wire contract. */
public final class ComputationCodec {
    private ComputationCodec() {}

    // Normalize and validate the operation before choosing a calculator or result type.
    public static String type(String type) {
        String normalized = type.toUpperCase(Locale.ROOT);
        if (!normalized.equals("MAX") && !normalized.equals("PRIMESUM") && !normalized.equals("PRIMECOUNT"))
            throw new IllegalArgumentException("Unsupported job type: " + type);
        return normalized;
    }

    // Translate the shared protobuf payload into validated calculator inputs.
    public static Object decode(String jobType, TaskPayload payload) {
        String type = type(jobType);
        return switch (payload.getContentCase()) {
            case PRIME_RANGE -> {
                if (type.equals("MAX")) throw new IllegalArgumentException("MAX requires numbers");
                yield new WorkerTaskCalculator.PrimeRange(payload.getPrimeRange().getStart(), payload.getPrimeRange().getEnd());
            }
            case INT_LIST -> {
                int[] values = payload.getIntList().getValuesList().stream().mapToInt(Integer::intValue).toArray();
                if (type.equals("MAX") && values.length == 0) throw new IllegalArgumentException("MAX requires at least one number");
                yield values;
            }
            case CONTENT_NOT_SET -> throw new IllegalArgumentException("Task payload is required");
        };
    }

    // Encode GUI inputs and distributed chunks using the same shared contract.
    public static TaskPayload encode(Object payload) {
        if (payload instanceof WorkerTaskCalculator.PrimeRange range)
            return TaskPayload.newBuilder().setPrimeRange(PrimeRangePayload.newBuilder()
                    .setStart(range.start()).setEnd(range.end())).build();
        IntList.Builder values = IntList.newBuilder();
        if (payload instanceof int[] array) for (int value : array) values.addValues(value);
        else if (payload instanceof Integer[] array) for (Integer value : array) values.addValues(value);
        else if (payload instanceof Collection<?> collection) for (Object value : collection) values.addValues((Integer)value);
        else throw new IllegalArgumentException("Expected numbers or a prime range");
        return TaskPayload.newBuilder().setIntList(values).build();
    }

    // Weight progress by input count, using long arithmetic for inclusive ranges.
    public static long units(Object payload) {
        return payload instanceof WorkerTaskCalculator.PrimeRange range
                ? (long)range.end() - range.start() + 1 : ((int[])payload).length;
    }

    // Preserve 64-bit sums and reject overflow for integer result types.
    public static TaskResult result(int jobId, int workerId, String type, Number value) {
        TaskResult.Builder reply = TaskResult.newBuilder().setJobId(jobId).setWorkerId(workerId);
        if (type(type).equals("PRIMESUM")) reply.setLongResult(value.longValue());
        else reply.setIntResult(Math.toIntExact(value.longValue()));
        return reply.build();
    }

    // Decode callbacks without narrowing long results or hiding worker errors.
    public static Object result(TaskResult result) {
        return switch (result.getResultCase()) {
            case INT_RESULT -> Integer.valueOf(result.getIntResult());
            case LONG_RESULT -> Long.valueOf(result.getLongResult());
            case ERROR -> result.getError();
            case RESULT_NOT_SET -> throw new IllegalArgumentException("Result is required");
        };
    }
}
