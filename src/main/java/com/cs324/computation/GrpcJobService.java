package com.cs324.computation;

import com.cs324.computation.proto.*;
import com.cs324.worker.WorkerTaskCalculator;
import com.cs324.worker.WorkerTaskDistributor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/** Coordinator computation service. Discovery, leadership and JAC belong to its host. */
public final class GrpcJobService extends JobServiceGrpc.JobServiceImplBase {
    public record Endpoint(int workerId, String host, int port) {
        public Endpoint {
            if (host == null || host.isBlank() || port < 1 || port > 65535)
                throw new IllegalArgumentException("A worker host and valid gRPC port are required");
        }
    }

    private final int coordinatorId;
    private final Supplier<List<Endpoint>> discovery;
    private final BooleanSupplier acceptsJobs;
    private final IntConsumer onAssignment;
    private final Executor executor;
    private final long taskTimeoutMillis;
    private final Set<String> activeJobs = ConcurrentHashMap.newKeySet();

    public GrpcJobService(int coordinatorId, Supplier<List<Endpoint>> discovery,
            BooleanSupplier acceptsJobs, IntConsumer onAssignment, Executor executor, Duration taskTimeout) {
        this.coordinatorId = coordinatorId;
        this.discovery = Objects.requireNonNull(discovery);
        this.acceptsJobs = Objects.requireNonNull(acceptsJobs);
        this.onAssignment = Objects.requireNonNull(onAssignment);
        this.executor = Objects.requireNonNull(executor);
        taskTimeoutMillis = taskTimeout.toMillis();
        if (taskTimeoutMillis < 1) throw new IllegalArgumentException("Task timeout must be positive");
    }

    @Override public void submitJob(JobRequest request, StreamObserver<JobUpdate> response) {
        final Object input;
        try {
            if (request.getJobId().isBlank()) throw new IllegalArgumentException("Job ID is required");
            input = ComputationCodec.decode(request.getType(), request.getInput());
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        if (!activeJobs.add(request.getJobId())) {
            response.onError(Status.ALREADY_EXISTS.withDescription("Job is already running").asRuntimeException());
            return;
        }
        Run run = new Run(request, response);
        try {
            if (!acceptsJobs.getAsBoolean()) {
                run.fail(Status.FAILED_PRECONDITION.withDescription("Coordinator is not accepting jobs"));
                return;
            }
        } catch (RuntimeException e) {
            run.fail(Status.UNAVAILABLE.withDescription("Coordinator admission failed").withCause(e));
            return;
        }
        if (response instanceof ServerCallStreamObserver<JobUpdate> stream)
            stream.setOnCancelHandler(run::cancel);
        // Discovery and local calculation must not occupy the gRPC request thread.
        try {
            executor.execute(() -> {
                try { run.start(input); }
                catch (Exception e) { run.fail(Status.UNAVAILABLE.withDescription("Could not distribute job").withCause(e)); }
            });
        } catch (RuntimeException e) {
            run.fail(Status.UNAVAILABLE.withDescription("Coordinator is stopping").withCause(e));
        }
    }

    private final class Run {
        final JobRequest request;
        final StreamObserver<JobUpdate> response;
        final List<TaskProgress> progress = new ArrayList<>();
        final List<Number> results = new ArrayList<>();
        final List<ManagedChannel> channels = new ArrayList<>();
        boolean finished;

        Run(JobRequest request, StreamObserver<JobUpdate> response) {
            this.request = request;
            this.response = response;
        }

        void start(Object input) {
            // Take a fresh membership snapshot per job; joining workers enter the next snapshot.
            Map<Integer, Endpoint> unique = new TreeMap<>();
            for (Endpoint endpoint : discovery.get()) {
                if (endpoint.workerId() != coordinatorId && unique.putIfAbsent(endpoint.workerId(), endpoint) != null)
                    throw new IllegalArgumentException("Duplicate worker ID in discovery");
            }
            List<Endpoint> peers = new ArrayList<>(unique.values());
            List<Object> chunks = WorkerTaskDistributor.split(request.getType().name(), input, peers.size() + 1);
            synchronized (this) {
                if (finished) return;
                for (int i = 0; i < chunks.size(); i++) {
                    progress.add(TaskProgress.newBuilder().setTaskId(request.getJobId() + "/" + i)
                            .setWorkerId(i == 0 ? coordinatorId : peers.get(i - 1).workerId())
                            .setTotalUnits(ComputationCodec.units(chunks.get(i))).build());
                }
                publish(false, null);
            }
            for (int i = 0; i < chunks.size(); i++) {
                final int index = i;
                Object chunk = chunks.get(i);
                synchronized (this) {
                    if (finished) return;
                    if (i == 0) {
                        executor.execute(() -> {
                            try { complete(index, WorkerTaskCalculator.calculate(request.getType().name(), chunk)); }
                            catch (Exception e) { fail(Status.INTERNAL.withDescription("Local calculation failed").withCause(e)); }
                        });
                    } else {
                        Endpoint peer = peers.get(i - 1);
                        ManagedChannel channel = ManagedChannelBuilder.forAddress(peer.host(), peer.port())
                                .usePlaintext().build();
                        channels.add(channel);
                        TaskRequest task = TaskRequest.newBuilder().setJobId(request.getJobId())
                                .setTaskId(progress.get(i).getTaskId()).setType(request.getType())
                                .setInput(ComputationCodec.encode(chunk)).build();
                        onAssignment.accept(peer.workerId());
                        TaskServiceGrpc.newStub(channel).withDeadlineAfter(taskTimeoutMillis,
                                java.util.concurrent.TimeUnit.MILLISECONDS).executeTask(task, new StreamObserver<>() {
                            private boolean received;
                            @Override public void onNext(TaskResult value) {
                                received = true;
                                if (!value.getJobId().equals(task.getJobId()) || !value.getTaskId().equals(task.getTaskId())
                                        || value.getWorkerId() != peer.workerId()) {
                                    fail(Status.DATA_LOSS.withDescription("Worker returned mismatched task identity"));
                                    return;
                                }
                                complete(index, value.getValue());
                            }
                            @Override public void onError(Throwable error) {
                                fail(Status.fromThrowable(error).augmentDescription("Task " + task.getTaskId()));
                            }
                            @Override public void onCompleted() {
                                if (!received) fail(Status.DATA_LOSS.withDescription("Worker returned no result"));
                            }
                        });
                    }
                }
            }
        }

        synchronized void complete(int index, Number result) {
            if (finished || progress.get(index).getCompleted()) return;
            TaskProgress task = progress.get(index);
            progress.set(index, task.toBuilder().setCompleted(true).setCompletedUnits(task.getTotalUnits()).build());
            results.add(result);
            if (results.size() == progress.size()) {
                try {
                    long total = WorkerTaskDistributor.combine(request.getType().name(), results).longValue();
                    publish(true, total);
                    finished = true;
                    cleanup();
                    response.onCompleted();
                } catch (Exception e) { fail(Status.INTERNAL.withDescription("Aggregation failed").withCause(e)); }
            } else publish(false, null);
        }

        void publish(boolean done, Long result) {
            JobUpdate.Builder update = JobUpdate.newBuilder().setJobId(request.getJobId())
                    .addAllTasks(progress).setFinished(done);
            if (result != null) update.setResult(result);
            response.onNext(update.build());
        }

        synchronized void fail(Status status) {
            if (finished) return;
            finished = true;
            cleanup();
            response.onError(status.asRuntimeException());
        }

        synchronized void cancel() {
            if (finished) return;
            finished = true;
            cleanup();
        }

        void cleanup() {
            channels.forEach(ManagedChannel::shutdownNow);
            activeJobs.remove(request.getJobId());
        }
    }
}
