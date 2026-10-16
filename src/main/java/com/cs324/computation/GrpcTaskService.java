package com.cs324.computation;

import com.cs324.computation.proto.*;
import com.cs324.worker.WorkerTaskCalculator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/** Runs each slice on the owning worker's computation pool. */
public final class GrpcTaskService extends TaskServiceGrpc.TaskServiceImplBase {
    private final int workerId;
    private final Executor executor;

    public GrpcTaskService(int workerId, Executor executor) {
        this.workerId = workerId;
        this.executor = executor;
    }

    @Override public void executeTask(TaskRequest request, StreamObserver<TaskResult> response) {
        final Object input;
        try {
            if (request.getJobId().isBlank() || request.getTaskId().isBlank())
                throw new IllegalArgumentException("Job and task IDs are required");
            input = ComputationCodec.decode(request.getType(), request.getInput());
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    long value = WorkerTaskCalculator.calculate(request.getType().name(), input).longValue();
                    response.onNext(TaskResult.newBuilder().setJobId(request.getJobId())
                            .setTaskId(request.getTaskId()).setWorkerId(workerId).setValue(value).build());
                    response.onCompleted();
                } catch (Exception e) {
                    response.onError(Status.INTERNAL.withDescription("Task calculation failed")
                            .withCause(e).asRuntimeException());
                }
            });
        } catch (RejectedExecutionException e) {
            response.onError(Status.UNAVAILABLE.withDescription("Worker is stopping").asRuntimeException());
        }
    }
}
