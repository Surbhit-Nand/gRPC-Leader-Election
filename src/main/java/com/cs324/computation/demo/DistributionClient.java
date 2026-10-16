package com.cs324.computation.demo;

import com.cs324.computation.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Console test client for the independent job-distribution feature. */
public final class DistributionClient {
    private DistributionClient() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3)
            throw new IllegalArgumentException("Usage: DistributionClient <host> <port> MAX|PRIMESUM|PRIMECOUNT [integers ...]");
        JobType type = JobType.valueOf(args[2].toUpperCase(Locale.ROOT));
        WorkInput.Builder input = WorkInput.newBuilder();
        if (type == JobType.PRIMESUM) {
            if (args.length != 5) throw new IllegalArgumentException("PRIMESUM requires start and end");
            input.setRange(PrimeRange.newBuilder().setStart(Integer.parseInt(args[3])).setEnd(Integer.parseInt(args[4])));
        } else if (type == JobType.MAX || type == JobType.PRIMECOUNT) {
            Numbers.Builder numbers = Numbers.newBuilder();
            for (int i = 3; i < args.length; i++) numbers.addValues(Integer.parseInt(args[i]));
            input.setNumbers(numbers);
        } else throw new IllegalArgumentException("Unsupported job type");
        JobRequest request = JobRequest.newBuilder().setJobId(UUID.randomUUID().toString())
                .setType(type).setInput(input).build();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(args[0], Integer.parseInt(args[1]))
                .usePlaintext().build();
        try {
            JobServiceGrpc.newBlockingStub(channel).withDeadlineAfter(11, TimeUnit.MINUTES)
                    .submitJob(request).forEachRemaining(update -> {
                        long total = update.getTasksList().stream().mapToLong(TaskProgress::getTotalUnits).sum();
                        long completed = update.getTasksList().stream().mapToLong(TaskProgress::getCompletedUnits).sum();
                        double percent = total == 0 ? (update.getFinished() ? 100 : 0) : 100.0 * completed / total;
                        System.out.printf(Locale.ROOT, "Job %s: %.1f%%%n", update.getJobId(), percent);
                        for (TaskProgress task : update.getTasksList())
                            System.out.printf("  Worker %d: %d/%d (%s)%n", task.getWorkerId(),
                                    task.getCompletedUnits(), task.getTotalUnits(), task.getCompleted() ? "done" : "running");
                        if (update.hasResult()) System.out.println("Result: " + update.getResult());
                    });
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
