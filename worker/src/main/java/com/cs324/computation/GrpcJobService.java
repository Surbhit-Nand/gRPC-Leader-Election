package com.cs324.computation;

import com.cs324.job.proto.*;
import com.cs324.common.BoundedMap;
import com.cs324.common.Constants;
import com.cs324.common.WorkerEvent;
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
    private static final long PROBE_INTERVAL_MILLIS = 500;
    private static final long PROBE_TIMEOUT_MILLIS = 1_000;
    private final int id;
    private final Supplier<CoordinatorEndpoint> self;
    private final Supplier<List<WorkerEndpoint>> discovery;
    private final IntPredicate admission;
    private final IntConsumer assignment;
    private final Runnable accepted;
    private final BiConsumer<String, String> events;
    private final Executor executor;
    private final ProgressService progress;
    private final long timeoutMillis;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "grpc-job-deadlines"); thread.setDaemon(true); return thread;
    });
    private final Map<Integer, Run> jobs = new ConcurrentHashMap<>();
    private record Submission(SubmitJobRequest request, CompletableFuture<Void> ready) {}
    private final BoundedMap<Integer, Submission> submissions = new BoundedMap<>(Constants.JOB_SUBMISSION_RETENTION);
    private record TaskKey(int job, int task, int attempt) {}
    /** Mutable on purpose: a new coordinator may take over where an attempt must report. */
    private static final class Work {
        volatile TaskAssignment request;
        final CompletableFuture<TaskResult> result = new CompletableFuture<>();
        Work(TaskAssignment request) { this.request = request; }
        TaskAssignment request() { return request; }
        CompletableFuture<TaskResult> result() { return result; }
    }
    private final BoundedMap<TaskKey, Work> tasks = new BoundedMap<>(Constants.JOB_TASK_RETENTION);
    // Replication is optional and additive: without a sink the job service behaves exactly
    // as before, and a failing sink can never fail a job.
    private volatile com.cs324.failover.CheckpointSink checkpoints = com.cs324.failover.CheckpointSink.noop();
    private volatile IntConsumer admissionRollback = jobId -> {};
    private volatile boolean closed;

    public void setAdmissionRollback(IntConsumer rollback) {
        admissionRollback = Objects.requireNonNull(rollback);
    }

    /** Lets Member 2 replicate coordinator state; additive and independent of job behaviour. */
    public void setCheckpointSink(com.cs324.failover.CheckpointSink sink) {
        this.checkpoints = sink == null ? com.cs324.failover.CheckpointSink.noop() : sink;
    }

    /** Observed through a wrapper so a replication fault cannot escape into the job path. */
    private void checkpoint(java.util.function.Consumer<com.cs324.failover.CheckpointSink> action) {
        try { action.accept(checkpoints); }
        catch (RuntimeException e) { System.err.println("Checkpoint update failed: " + e.getMessage()); }
    }

    public GrpcJobService(int id, Supplier<CoordinatorEndpoint> self, Supplier<List<WorkerEndpoint>> discovery,
            IntPredicate admission, IntConsumer assignment, Runnable accepted, Executor executor,
            ProgressService progress, Duration timeout) {
        this(id, self, discovery, admission, assignment, accepted, executor, progress, timeout, (kind, detail) -> {});
    }

    public GrpcJobService(int id, Supplier<CoordinatorEndpoint> self, Supplier<List<WorkerEndpoint>> discovery,
            IntPredicate admission, IntConsumer assignment, Runnable accepted, Executor executor,
            ProgressService progress, Duration timeout, BiConsumer<String, String> events) {
        this.events=Objects.requireNonNull(events);
        this.id=id; this.self=self; this.discovery=discovery; this.admission=admission;
        this.assignment=assignment; this.accepted=accepted; this.executor=executor; this.progress=progress;
        timeoutMillis=timeout.toMillis();
        if (timeoutMillis < 1) throw new IllegalArgumentException("Positive timeout required");
    }

    // Observability must never interrupt recovery or aggregation.
    private void event(String kind, String detail) {
        try { events.accept(kind, detail); }
        catch (RuntimeException e) { System.err.println("Cannot record job event: " + e.getMessage()); }
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
            boolean admitted = false;
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
                admitted = true;
                run = new Run(request, chunks, new ArrayList<>(members.values()));
                jobs.put(request.getJobId(), run);
                run.begin();
                executor.execute(run::dispatch);
            } catch (RuntimeException e) {
                submissions.remove(request.getJobId(), claim);
                claim.ready().completeExceptionally(status(e));
                Run failed = jobs.remove(request.getJobId());
                if (failed != null) failed.abortSubmission();
                if (admitted) admissionRollback.accept(request.getJobId());
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
            TaskKey key = new TaskKey(request.getJobId(), request.getTaskId(), request.getAttempt());
            Work work = new Work(request);
            Work previous = tasks.putIfAbsent(key, work);
            if (previous != null && !previous.request().equals(request)) {
                // Same (job, task, attempt) from a different coordinator: the work is already
                // done or in flight, so recomputing would waste it and answering the old
                // coordinator would lose the result. Move the report target instead.
                if (sameAttemptNewCoordinator(previous.request(), request)) {
                    previous.request = request;
                    if (previous.result().isDone()) report(request, previous.result().join());
                    ack(response, TaskAck.getDefaultInstance());
                    return;
                }
                throw Status.ALREADY_EXISTS.withDescription("Conflicting task attempt").asRuntimeException();
            }
            if (previous == null) {
                try {
                    Object input = ComputationCodec.decode(request.getJobType(), request.getPayload());
                    executor.execute(() -> {
                        TaskResult result = calculate(request.getJobId(), request.getJobType(), input).toBuilder()
                                .setTaskId(request.getTaskId()).setAttempt(request.getAttempt()).build();
                        work.result().complete(result);
                        report(work.request(), result);
                    });
                } catch (RuntimeException e) { tasks.remove(key, work); throw e; }
            } else if (previous.result().isDone()) {
                // Repeated assignment probes liveness and retries a lost result callback.
                report(request, previous.result().join());
            }
            ack(response, TaskAck.getDefaultInstance());
        } catch (Exception e) { response.onError(status(e)); }
    }

    /** Identical work with only the reporting coordinator replaced, which a failover does. */
    private static boolean sameAttemptNewCoordinator(TaskAssignment held, TaskAssignment incoming) {
        return held.getJobId() == incoming.getJobId() && held.getTaskId() == incoming.getTaskId()
                && held.getAttempt() == incoming.getAttempt() && held.getJobType().equals(incoming.getJobType())
&& held.getPayload().equals(incoming.getPayload());
    }

    private void report(TaskAssignment request, TaskResult result) {
        if (closed) return;
        CoordinatorEndpoint coordinator = request.getCoordinator();
        this.<TaskResultAck>send(coordinator.getHost(), coordinator.getPort(),
                (channel, observer) -> stub(channel).reportTaskResult(result, observer),
                error -> System.err.println("Result delivery will retry on the next probe for job " + request.getJobId()));
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

    // Stable slice IDs identify work; attempts fence callbacks from previous owners.
    private final class Run {
        final SubmitJobRequest request;
        final List<Slice> slices = new ArrayList<>();
        final Set<Integer> failedWorkers = new HashSet<>();
        // When the job was accepted, so a restored job keeps the deadline it would have had.
        final long acceptedAtMillis = System.currentTimeMillis();
        ScheduledFuture<?> deadline, probes;
        boolean finished;
        final class Slice {
            final int taskId;
            final Object input;
            WorkerEndpoint owner;
            int attempt = 1;
            boolean probing, recovering, started;
            TaskAssignment wireTask;
            Number result;
            Slice(int taskId, Object input, WorkerEndpoint owner) {
                this.taskId=taskId; this.input=input; this.owner=owner;
            }
        }
        Run(SubmitJobRequest request, List<Object> chunks, List<WorkerEndpoint> peers) {
            this.request=request;
            WorkerEndpoint local = WorkerEndpoint.newBuilder().setWorkerId(id).build();
            for (int i=0; i<chunks.size(); i++) slices.add(new Slice(i+1, chunks.get(i), i==0?local:peers.get(i-1)));
        }
        /** Rebuilds a job a dead coordinator owned; every field comes from the replicated log. */
        Run(com.cs324.failover.RestoredJob restored) {
            this.request = restored.request();
            for (com.cs324.failover.RestoredJob.RestoredSlice slice : restored.slices()) {
                Slice copy = new Slice(slice.taskId(),
                        ComputationCodec.decode(request.getJobType(), slice.payload()), slice.owner());
                copy.attempt = Math.max(slice.attempt(), 1);
                if (slice.result() != null) {
                    // Already answered before the failure: keep the answer so the job neither
                    // recomputes it nor counts it twice.
                    Object value = ComputationCodec.result(slice.result());
                    if (value instanceof Number number) copy.result = number;
                }
                slices.add(copy);
            }
        }
        private void log(String kind, String detail) {
            event(kind, detail + " [" + ComputationCodec.type(request.getJobType()) + "]");
        }
        void begin() {
            // Acceptance is a required operation, not a best-effort telemetry update.
            // No work, progress or callbacks may start before a replica confirms it.
            checkpoints.onAccepted(request, checkpointSlices(), acceptedAtMillis);
            log("JOB_SUBMITTED", "Job " + request.getJobId() + " submitted via gRPC to coordinator Worker " + id);
            publishProgress();
            deadline=timer.schedule(() -> fail("Job timed out waiting for worker results"), timeoutMillis, TimeUnit.MILLISECONDS);
            probes=timer.scheduleWithFixedDelay(this::probe, PROBE_INTERVAL_MILLIS, PROBE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
        synchronized void abortSubmission() {
            finished = true;
            if (deadline != null) deadline.cancel(false);
            if (probes != null) probes.cancel(false);
        }
        /** The accepted split as replication needs it: identity, input, owner and attempt. */
        private List<com.cs324.failover.CheckpointSink.SliceSnapshot> checkpointSlices() {
            List<com.cs324.failover.CheckpointSink.SliceSnapshot> snapshot =
                    new ArrayList<>(slices.size());
            for (Slice slice : slices)
                snapshot.add(new com.cs324.failover.CheckpointSink.SliceSnapshot(slice.taskId,
                        ComputationCodec.encode(slice.input), slice.owner, slice.attempt));
            return snapshot;
        }
        /** Publishes one slice transition so a survivor knows where the work went. */
        private void checkpointSlice(Slice slice, TaskResult result) {
            checkpoint(sink -> sink.onSliceUpdated(request.getJobId(), slice.taskId,
                    slice.owner, slice.attempt, result));
        }
        /** Restarts a recovered job: the original deadline is not extended, only its floor. */
        void beginRestored(long remainingMillis) {
            long pending = slices.stream().filter(slice -> slice.result==null).count();
            log(WorkerEvent.JOB_RECOVERED, "Job " + request.getJobId() + " restored on coordinator Worker "
                    + id + " with " + pending + " unfinished slice(s)");
            publishProgress();
            deadline=timer.schedule(() -> fail("Job timed out waiting for worker results"),
                    Math.max(remainingMillis, 1L), TimeUnit.MILLISECONDS);
            probes=timer.scheduleWithFixedDelay(this::probe, PROBE_INTERVAL_MILLIS, PROBE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
        synchronized void dispatch() {
            try {
                for (Slice slice : slices) dispatch(slice);
                finishIfComplete();
            }
            catch (RuntimeException e) { fail("Dispatch failed: " + e.getMessage()); }
        }
        private void dispatch(Slice slice) {
            if (finished || slice.result != null) return;
            if (slice.recovering) return;
            slice.started=true;
            log(WorkerEvent.TASK_DISPATCHED, "Job " + request.getJobId() + ", task " + slice.taskId
                    + ": " + (slice.owner.getWorkerId()==id ? "slice kept by Worker " : "slice distributed via gRPC to Worker ")
                    + slice.owner.getWorkerId());
            int attempt = slice.attempt;
            if (slice.owner.getWorkerId()==id) {
                executor.execute(() -> complete(calculate(request.getJobId(), request.getJobType(), slice.input)
                        .toBuilder().setTaskId(slice.taskId).setAttempt(attempt).build()));
            } else {
                assignment.accept(slice.owner.getWorkerId());
                checkpointSlice(slice, null);
                sendAssignment(slice);
            }
        }
        synchronized void probe() {
            if (finished) return;
            for (Slice slice : slices)
                if (slice.started && slice.result==null && slice.owner.getWorkerId()!=id && !slice.probing && !slice.recovering) sendAssignment(slice);
        }
        private void sendAssignment(Slice slice) {
            int attempt=slice.attempt;
            slice.probing=true;
            // Rebuild when the attempt or the reporting coordinator changed: after a failover
            // the same attempt must carry the new endpoint or results go to the dead leader.
            if (slice.wireTask==null || slice.wireTask.getAttempt()!=attempt
                    || !slice.wireTask.getCoordinator().equals(self.get())) slice.wireTask=TaskAssignment.newBuilder()
                    .setJobId(request.getJobId()).setTaskId(slice.taskId).setAttempt(attempt)
                    .setJobType(request.getJobType()).setPayload(ComputationCodec.encode(slice.input))
                    .setCoordinator(self.get()).build();
            TaskAssignment task=slice.wireTask;
            GrpcJobService.this.<TaskAck>send(slice.owner.getHost(), slice.owner.getGrpcPort(),
                    (channel, observer) -> JobServiceGrpc.newStub(channel).withDeadlineAfter(PROBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                            .assignTask(task, new StreamObserver<>() {
                                public void onNext(TaskAck ack) { observer.onNext(ack); }
                                public void onError(Throwable error) { observer.onError(error); }
                                public void onCompleted() {
                                    synchronized (Run.this) { if (slice.attempt==attempt) slice.probing=false; }
                                    observer.onCompleted();
                                }
                            }), error -> recover(slice, attempt));
        }
        private void recover(Slice slice, int attempt) {
            synchronized (this) {
                if (finished || slice.result!=null || slice.attempt!=attempt) return;
                failedWorkers.add(slice.owner.getWorkerId());
                log(WorkerEvent.WORKER_FAILED, "Job " + request.getJobId() + ", task " + slice.taskId
                        + ": Worker " + slice.owner.getWorkerId() + " failed or unreachable; recovering unfinished task");
                slice.attempt++;
                slice.recovering=true;
            }
            try { executor.execute(() -> {
                List<WorkerEndpoint> available;
                try { available=discovery.get(); }
                catch (RuntimeException unavailable) { available=List.of(); }
                synchronized (Run.this) {
                    if (finished || slice.result!=null) return;
                    int previousOwner=slice.owner.getWorkerId();
                    // Prefer idle/new remote members; the coordinator is a fallback.
                    slice.owner=available.stream().filter(w -> w.getWorkerId()!=id && !failedWorkers.contains(w.getWorkerId()))
                            .min(Comparator.comparingLong(w -> slices.stream()
                                    .filter(s -> s.result==null && s.owner.getWorkerId()==w.getWorkerId()).count()))
                            .orElse(WorkerEndpoint.newBuilder().setWorkerId(id).build());
                    System.out.printf("Job %d task %d: worker %d unavailable; reassigned to worker %d (attempt %d)%n",
                            request.getJobId(), slice.taskId, previousOwner, slice.owner.getWorkerId(), slice.attempt);
                    log(WorkerEvent.TASK_REASSIGNED, "Job " + request.getJobId() + ", task " + slice.taskId
                            + ": reassigned from Worker " + previousOwner + " to Worker " + slice.owner.getWorkerId()
                            + " (attempt " + slice.attempt + ")");
                    slice.probing=false;
                    slice.recovering=false;
                    publishProgress();
                    checkpointSlice(slice, null);
                    try { dispatch(slice); }
                    catch (RuntimeException e) { fail("Recovery dispatch failed: " + e.getMessage()); }
                }
            }); } catch (RuntimeException e) { fail("Recovery could not start: " + e.getMessage()); }
        }
        synchronized void complete(TaskResult result) {
            if (finished) return;
            Slice slice=slices.stream().filter(s -> s.taskId==result.getTaskId()).findFirst().orElse(null);
            if (slice==null || slice.result!=null || slice.attempt!=result.getAttempt() || slice.recovering
                    || slice.owner.getWorkerId()!=result.getWorkerId()) return;
            Object value=ComputationCodec.result(result);
            if (!(value instanceof Number number)) { fail(String.valueOf(value)); return; }
            if (ComputationCodec.type(request.getJobType()).equals("PRIMESUM") != result.hasLongResult()) {
                fail("Worker returned an incorrect numeric result type"); return;
            }
            slice.result=number;
            log(WorkerEvent.TASK_COMPLETED, "Job " + request.getJobId() + ", task " + slice.taskId
                    + ": " + (slice.attempt>1 ? "reassigned task completed" : "task completed")
                    + " by Worker " + result.getWorkerId() + " (attempt " + slice.attempt + ")");
            publishProgress();
            checkpointSlice(slice, result);
            finishIfComplete();
        }
        private void finishIfComplete() {
            if (!finished && slices.stream().allMatch(s -> s.result!=null)) {
                try { finish(ComputationCodec.result(request.getJobId(), id, request.getJobType(),
                        WorkerTaskDistributor.combine(request.getJobType(), slices.stream().map(s -> s.result).toList()))); }
                catch (Exception e) { fail("Aggregation failed: " + e.getMessage()); }
            }
        }
        private void publishProgress() {
            Map<Integer, Long> totals=new LinkedHashMap<>(), completed=new HashMap<>();
            Set<Integer> pending=new HashSet<>();
            for (Slice slice : slices) {
                long units=ComputationCodec.units(slice.input);
                totals.merge(slice.owner.getWorkerId(), units, Long::sum);
                if (slice.result!=null) completed.merge(slice.owner.getWorkerId(), units, Long::sum);
                else pending.add(slice.owner.getWorkerId());
            }
            progress.update(request.getJobId(), totals, completed, pending);
        }
        synchronized void fail(String message) {
            if (finished) return;
            for (Slice slice : slices) if (slice.result==null) progress.complete(request.getJobId(), slice.owner.getWorkerId(), false);
            finish(error(request.getJobId(), message));
        }
        void finish(TaskResult result) {
            if (finished) return;
            finished=true;
            log(result.hasError() ? "JOB_FAILED" : "JOB_COMPLETED", "Job " + request.getJobId()
                    + (result.hasError() ? " failed: " + result.getError() : " completed; sending result to client via gRPC"));
            if (deadline!=null) deadline.cancel(false);
            if (probes!=null) probes.cancel(false);
            jobs.remove(request.getJobId(), this);
            // Tombstone first, then the delivery outcome: a survivor that reads the log between
            // those two steps sees a finished job whose delivery is still unknown, and replays it.
            checkpoint(sink -> sink.onFinished(request.getJobId(), result));
            ClientEndpoint client=request.getClient();
            int jobId=request.getJobId();
            GrpcJobService.this.<DeliverResultAck>send(client.getHost(), client.getPort(), (channel, observer) ->
                    ClientServiceGrpc.newStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
                            .deliverResult(result, new StreamObserver<>() {
                        public void onNext(DeliverResultAck ack) { }
                        public void onError(Throwable error) {
                            checkpoint(sink -> sink.onDeliveryFailed(jobId));
                            observer.onError(error);
                        }
                        public void onCompleted() {
                            checkpoint(sink -> sink.onDelivered(jobId));
                            observer.onCompleted();
                        }
                    }), error -> {
                        checkpoint(sink -> sink.onDeliveryFailed(jobId));
                        System.err.println("Cannot deliver job " + jobId + " result: " + error);
                    });
        }
    }

    /**
     * Resumes a job whose coordinator died, from state read back out of the replicated log.
     *
     * <p>Additive and outside the admission predicate on purpose: a recovered job was already
     * accepted and billed to the client, so re-checking the five-job budget here would either
     * refuse work that is already owed to a client or charge the new term twice. It also
     * registers the original request in {@code submissions}, so a client that resubmits the
     * same job id is acknowledged instead of running the work again.
     *
     * @return false when this job id is already being coordinated here.
     */
    public boolean restore(com.cs324.failover.RestoredJob restored, long remainingMillis) {
        try {
            SubmitJobRequest request = restored.request();
            if (request.getJobId() < 1) throw new IllegalArgumentException("Positive job ID required");
            endpoint(request.getClient().getHost(), request.getClient().getPort());
            if (jobs.containsKey(request.getJobId())) return false;
            // Identity claim first: a concurrent duplicate submission then attaches to this
            // job instead of starting a second run of the same work.
            Submission claim = new Submission(request, CompletableFuture.completedFuture(null));
            if (submissions.putIfAbsent(request.getJobId(), claim) != null) return false;
            Run run;
            try {
                run = new Run(restored);
                if (run.slices.isEmpty()) throw new IllegalArgumentException("A restored job needs its slices");
                jobs.put(request.getJobId(), run);
                run.beginRestored(remainingMillis);
                executor.execute(run::dispatch);
            } catch (RuntimeException e) {
                submissions.remove(request.getJobId(), claim);
                Run failed = jobs.remove(request.getJobId());
                if (failed != null) failed.fail("Restored job could not start: " + e.getMessage());
                return false;
            }
            return true;
        } catch (RuntimeException e) {
            System.err.println("Cannot restore job: " + e.getMessage());
            return false;
        }
    }

    /** Sends one already-computed result to its client, used when replaying a tombstone. */
    public boolean deliverResult(SubmitJobRequest request, TaskResult result) {
        try {
            ClientEndpoint client = request.getClient();
            endpoint(client.getHost(), client.getPort());
            ManagedChannel channel = GrpcChannels.get(client.getHost(), client.getPort());
            // Takeover runs on the worker pool. Only a received acknowledgement proves
            // delivery; scheduling an asynchronous RPC does not.
            ClientServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
                    .deliverResult(result);
            return true;
        } catch (RuntimeException e) {
            System.err.println("Cannot replay job result: " + e.getMessage());
            return false;
        }
    }

    /** Job ids this coordinator currently owns, so a failover can tell taken from untouched. */
    public Set<Integer> coordinatedJobIds() {
        return Set.copyOf(jobs.keySet());
    }

    private TaskResult error(int jobId, String text) {
        return TaskResult.newBuilder().setJobId(jobId).setWorkerId(id).setError(text).build();
    }
    private static JobServiceGrpc.JobServiceStub stub(ManagedChannel channel) {
        return JobServiceGrpc.newStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }
    private <T> void send(String host, int port, BiConsumer<ManagedChannel, StreamObserver<T>> call, Consumer<Throwable> failure) {
        if (closed) return;
        ManagedChannel channel = GrpcChannels.get(host, port);
        try {
            // Acknowledging ReportTaskResult ends its inbound RPC context. The
            // independent client callback must survive that acknowledgement.
            Context.ROOT.run(() -> call.accept(channel, new StreamObserver<>() {
                public void onNext(T value) { }
                public void onError(Throwable error) { failure.accept(error); }
                public void onCompleted() { }
            }));
        } catch (RuntimeException e) { failure.accept(e); }
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
    @Override public void close() { closed = true; timer.shutdownNow(); }
}
