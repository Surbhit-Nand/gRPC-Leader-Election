package com.cs324.computation;

import com.cs324.job.proto.*;
import com.cs324.registry.proto.WorkerEndpoint;
import com.cs324.progress.ProgressService;
import com.cs324.worker.WorkerTaskCalculator;
import com.cs324.worker.WorkerTaskDistributor;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Shared SubmitJob/AssignTask/ReportTaskResult API, independent of election transport. */
public final class GrpcJobService extends JobServiceGrpc.JobServiceImplBase implements AutoCloseable {
    private final int id;
    private final Supplier<CoordinatorEndpoint> self;
    private final Supplier<List<WorkerEndpoint>> discovery;
    private final IntPredicate admission;
    private final IntConsumer assignment;
    private final Runnable accepted;
    private final Executor executor;
    private final ProgressService progress;
    private final long timeoutMillis;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "grpc-job-deadlines"); thread.setDaemon(true); return thread;
    });
    private final Map<Integer, Run> jobs = new ConcurrentHashMap<>();
    private record Submission(SubmitJobRequest request, CompletableFuture<Void> ready) {}
    private final Map<Integer, Submission> submissions = new ConcurrentHashMap<>();
    private final Map<Integer, TaskAssignment> tasks = new ConcurrentHashMap<>();
    private final Set<ManagedChannel> channels = ConcurrentHashMap.newKeySet();

    public GrpcJobService(int id, Supplier<CoordinatorEndpoint> self, Supplier<List<WorkerEndpoint>> discovery,
            IntPredicate admission, IntConsumer assignment, Runnable accepted, Executor executor,
            ProgressService progress, Duration timeout) {
        this.id=id; this.self=self; this.discovery=discovery; this.admission=admission;
        this.assignment=assignment; this.accepted=accepted; this.executor=executor; this.progress=progress;
        timeoutMillis=timeout.toMillis();
        if (timeoutMillis < 1) throw new IllegalArgumentException("Positive timeout required");
    }

    // Claim each job once, snapshot ready workers, and acknowledge after execution is scheduled.
    @Override public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> response) {
        try {
            if (request.getJobId() < 1) throw new IllegalArgumentException("Positive job ID required");
            endpoint(request.getClient().getHost(), request.getClient().getPort());
            Object payload = ComputationCodec.decode(request.getJobType(), request.getPayload());
            Submission claim = new Submission(request, new CompletableFuture<>());
            Submission previous = submissions.putIfAbsent(request.getJobId(), claim);
            if (previous != null) {
                if (!previous.request().equals(request)) throw Status.ALREADY_EXISTS.withDescription("Job ID belongs to another submission").asRuntimeException();
                previous.ready().whenComplete((ignored, failure) -> {
                    if (failure == null) ack(response, SubmitJobResponse.getDefaultInstance());
                    else response.onError(failure);
                });
                return;
            }
            Run run;
            try {
                TreeMap<Integer, WorkerEndpoint> members = new TreeMap<>();
                for (WorkerEndpoint peer : discovery.get()) {
                    if (peer.getWorkerId() != id) {
                        endpoint(peer.getHost(), peer.getGrpcPort());
                        if (members.putIfAbsent(peer.getWorkerId(), peer) != null)
                            throw new IllegalArgumentException("Duplicate worker ID in discovery");
                    }
                }
                List<Object> chunks = WorkerTaskDistributor.split(request.getJobType(), payload, members.size()+1);
                if (!admission.test(request.getJobId()))
                    throw Status.FAILED_PRECONDITION.withDescription("Worker is not accepting coordinator jobs").asRuntimeException();
                run = new Run(request, chunks, new ArrayList<>(members.values()));
                jobs.put(request.getJobId(), run);
                run.begin();
                executor.execute(run::dispatch);
            } catch (RuntimeException e) {
                submissions.remove(request.getJobId(), claim);
                claim.ready().completeExceptionally(status(e));
                Run failed = jobs.remove(request.getJobId());
                if (failed != null) failed.fail("Job dispatch could not start: " + e.getMessage());
                throw e;
            }
            claim.ready().complete(null);
            ack(response, SubmitJobResponse.getDefaultInstance());
            try { accepted.run(); }
            catch (RuntimeException e) { System.err.println("Coordinator handover hook failed: " + e.getMessage()); }
        } catch (Exception e) { response.onError(status(e)); }
    }

    // Run shares on the worker pool; duplicate assignments must not repeat the calculation.
    @Override public void assignTask(TaskAssignment request, StreamObserver<TaskAck> response) {
        try {
            if (request.getJobId() < 1) throw new IllegalArgumentException("Positive job ID required");
            endpoint(request.getCoordinator().getHost(), request.getCoordinator().getPort());
            Object input = ComputationCodec.decode(request.getJobType(), request.getPayload());
            TaskAssignment previous = tasks.putIfAbsent(request.getJobId(), request);
            if (previous != null && !previous.equals(request))
                throw Status.ALREADY_EXISTS.withDescription("Conflicting task for job ID").asRuntimeException();
            if (previous == null) {
                try {
                    executor.execute(() -> {
                        TaskResult result = calculate(request.getJobId(), request.getJobType(), input);
                        CoordinatorEndpoint coordinator = request.getCoordinator();
                        this.<TaskResultAck>send(coordinator.getHost(), coordinator.getPort(),
                                (channel, observer) -> stub(channel).reportTaskResult(result, observer),
                                error -> System.err.println("Cannot report job " + request.getJobId() + ": " + error));
                    });
                } catch (RuntimeException e) { tasks.remove(request.getJobId(), request); throw e; }
            }
            ack(response, TaskAck.getDefaultInstance());
        } catch (Exception e) { response.onError(status(e)); }
    }

    // Route shares to the original coordinator, including after a normal leader rotation.
    @Override public void reportTaskResult(TaskResult result, StreamObserver<TaskResultAck> response) {
        try {
            ComputationCodec.result(result);
            Run run = jobs.get(result.getJobId());
            if (run != null) run.complete(result);
            // Completed jobs safely acknowledge duplicate or late results.
            ack(response, TaskResultAck.getDefaultInstance());
        } catch (Exception e) { response.onError(status(e)); }
    }

    // Return calculation errors through the same callback contract as successful results.
    private TaskResult calculate(int jobId, String type, Object input) {
        try { return ComputationCodec.result(jobId, id, type, WorkerTaskCalculator.calculate(type, input)); }
        catch (Exception e) { return error(jobId, "Calculation failed: " + e.getMessage()); }
    }

    // Per-job state: synchronized result handling prevents duplicate aggregation and delivery.
    private final class Run {
        final SubmitJobRequest request;
        final List<Object> chunks;
        final List<WorkerEndpoint> peers;
        final Map<Integer, Long> units = new LinkedHashMap<>();
        final Map<Integer, Number> results = new HashMap<>();
        ScheduledFuture<?> deadline;
        boolean finished;
        Run(SubmitJobRequest request, List<Object> chunks, List<WorkerEndpoint> peers) {
            this.request=request; this.chunks=chunks; this.peers=peers;
            for (int i=0;i<chunks.size();i++) units.put(i==0?id:peers.get(i-1).getWorkerId(), ComputationCodec.units(chunks.get(i)));
        }
        // Publish assigned work before dispatch and bound the wait for missing results.
        void begin() {
            progress.begin(request.getJobId(), units);
            deadline=timer.schedule(() -> fail("Job timed out waiting for worker results"), timeoutMillis, TimeUnit.MILLISECONDS);
        }
        // Keep the first share locally and send remaining shares to discovered gRPC endpoints.
        void dispatch() {
            try {
                for (int i=0;i<chunks.size();i++) {
                    synchronized (this) { if (finished) return; }
                    Object chunk=chunks.get(i);
                    if (i==0) executor.execute(() -> complete(calculate(request.getJobId(), request.getJobType(), chunk)));
                    else {
                        WorkerEndpoint peer=peers.get(i-1);
                        TaskAssignment task=TaskAssignment.newBuilder().setJobId(request.getJobId())
                                .setJobType(request.getJobType()).setPayload(ComputationCodec.encode(chunk)).setCoordinator(self.get()).build();
                        assignment.accept(peer.getWorkerId());
                        GrpcJobService.this.<TaskAck>send(peer.getHost(), peer.getGrpcPort(), (channel, observer) -> stub(channel).assignTask(task, observer),
                                error -> fail("Worker " + peer.getWorkerId() + " assignment failed: " + Status.fromThrowable(error).getCode()));
                    }
                }
            } catch (Exception e) { fail("Dispatch failed: " + e.getMessage()); }
        }
        // Count each worker once; combine only when every expected share has returned.
        synchronized void complete(TaskResult result) {
            if (finished || results.containsKey(result.getWorkerId())) return;
            if (!units.containsKey(result.getWorkerId()))
                throw Status.INVALID_ARGUMENT.withDescription("Worker is not assigned to this job").asRuntimeException();
            Object value=ComputationCodec.result(result);
            if (!(value instanceof Number number)) { fail(String.valueOf(value)); return; }
            boolean longExpected=ComputationCodec.type(request.getJobType()).equals("PRIMESUM");
            if (longExpected != result.hasLongResult()) { fail("Worker returned an incorrect numeric result type"); return; }
            results.put(result.getWorkerId(), number);
            progress.complete(request.getJobId(), result.getWorkerId(), true);
            if (results.size()==units.size()) {
                try { finish(ComputationCodec.result(request.getJobId(), id, request.getJobType(),
                        WorkerTaskDistributor.combine(request.getJobType(), results.values()))); }
                catch (Exception e) { fail("Aggregation failed: " + e.getMessage()); }
            }
        }
        // End unfinished shares explicitly. Reassignment belongs to the separate recovery feature.
        synchronized void fail(String message) {
            if (finished) return;
            for (int workerId:units.keySet()) if (!results.containsKey(workerId)) progress.complete(request.getJobId(), workerId, false);
            finish(error(request.getJobId(), message));
        }
        // Release active state and deliver one final result to the submitting client.
        void finish(TaskResult result) {
            if (finished) return;
            finished=true;
            if (deadline!=null) deadline.cancel(false);
            jobs.remove(request.getJobId(), this);
            ClientEndpoint client=request.getClient();
            GrpcJobService.this.<DeliverResultAck>send(client.getHost(), client.getPort(), (channel, observer) -> ClientServiceGrpc.newStub(channel)
                    .withDeadlineAfter(10, TimeUnit.SECONDS).deliverResult(result, observer),
                    error -> System.err.println("Cannot deliver job " + request.getJobId() + " result: " + error));
        }
    }

    private TaskResult error(int jobId, String text) {
        return TaskResult.newBuilder().setJobId(jobId).setWorkerId(id).setError(text).build();
    }
    private static JobServiceGrpc.JobServiceStub stub(ManagedChannel channel) {
        return JobServiceGrpc.newStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }
    private <T> void send(String host, int port, BiConsumer<ManagedChannel, StreamObserver<T>> call, Consumer<Throwable> failure) {
        ManagedChannel channel=ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        channels.add(channel);
        try {
            // Acknowledging ReportTaskResult ends its inbound RPC context. The
            // independent client callback must survive that acknowledgement.
            Context.ROOT.run(() -> call.accept(channel, new StreamObserver<>() {
                public void onNext(T value) { }
                public void onError(Throwable error) { release(); failure.accept(error); }
                public void onCompleted() { release(); }
                private void release() { channels.remove(channel); channel.shutdown(); }
            }));
        } catch (RuntimeException e) { channels.remove(channel); channel.shutdownNow(); failure.accept(e); }
    }
    private static void endpoint(String host,int port) {
        if (host.isBlank() || port<1 || port>65535) throw new IllegalArgumentException("A valid endpoint is required");
    }
    private static <T> void ack(StreamObserver<T> response,T reply) { response.onNext(reply); response.onCompleted(); }
    private static Throwable status(Exception error) {
        if (error instanceof StatusRuntimeException) return error;
        return (error instanceof IllegalArgumentException ? Status.INVALID_ARGUMENT : Status.UNAVAILABLE)
                .withDescription(error.getMessage()).withCause(error).asRuntimeException();
    }
    @Override public void close() { timer.shutdownNow(); channels.forEach(ManagedChannel::shutdownNow); channels.clear(); }
}
