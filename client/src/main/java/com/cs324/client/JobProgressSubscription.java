package com.cs324.client;

import com.cs324.progress.proto.*;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.*;

// Re-discovers the job owner after interruption; never resubmits computation.
final class JobProgressSubscription implements AutoCloseable {
    private final int jobId;
    private final WorkerEndpoint initial;
    private final Supplier<List<WorkerEndpoint>> discovery;
    private final BooleanSupplier visible;
    private final Consumer<JobProgress> receive;
    private final Runnable interrupted;
    private final Consumer<JobProgressSubscription> ended;
    private final ScheduledExecutorService retries = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "job-progress-reconnect"); thread.setDaemon(true); return thread;
    });
    private ManagedChannel channel;
    private boolean stopped;
    private volatile boolean cancelled;
    private int nextPeer;

    JobProgressSubscription(int jobId, WorkerEndpoint initial, Supplier<List<WorkerEndpoint>> discovery,
            BooleanSupplier visible, Consumer<JobProgress> receive, Runnable interrupted,
            Consumer<JobProgressSubscription> ended) {
        this.jobId = jobId; this.initial = initial; this.discovery = discovery; this.visible = visible;
        this.receive = receive; this.interrupted = interrupted; this.ended = ended;
    }
    void start() { retries.execute(() -> connect(initial)); }
    boolean cancelled() { return cancelled; }

    private void connect(WorkerEndpoint endpoint) {
        final ManagedChannel attempt;
        synchronized (this) {
            if (stopped) return;
            if (!visible.getAsBoolean()) { close(); return; }
            attempt = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getGrpcPort()).usePlaintext().build();
            channel = attempt;
        }
        // Stream deadlines detect silent connections; reconnecting replays progress without resubmitting work.
        Context.ROOT.run(() -> JobProgressServiceGrpc.newStub(attempt).withDeadlineAfter(30, TimeUnit.SECONDS)
                .watchJob(WatchJobRequest.newBuilder().setJobId(jobId).build(), new StreamObserver<>() {
                    private boolean finished;
                    public void onNext(JobProgress snapshot) {
                        finished = snapshot.getFinished();
                        if (!cancelled) receive.accept(snapshot);
                    }
                    public void onError(Throwable error) { release(attempt); retry(); }
                    public void onCompleted() {
                        release(attempt);
                        if (finished) stop(false); else retry();
                    }
                }));
    }

    private synchronized void release(ManagedChannel attempt) {
        if (channel == attempt) channel = null;
        attempt.shutdownNow();
    }
    private synchronized void retry() {
        if (stopped) return;
        if (!visible.getAsBoolean()) { close(); return; }
        interrupted.run();
        retries.schedule(() -> {
            List<WorkerEndpoint> peers;
            try { peers = discovery.get().stream().filter(p -> p.getGrpcPort() > 0)
                    .sorted(java.util.Comparator.comparingInt(WorkerEndpoint::getWorkerId)).toList(); }
            catch (RuntimeException unavailable) { peers = List.of(); }
            // After leadership rotation, unfinished jobs stay with their original coordinator.
            connect(peers.isEmpty() ? initial : peers.get(Math.floorMod(nextPeer++, peers.size())));
        }, 500, TimeUnit.MILLISECONDS);
    }
    private synchronized void stop(boolean cancel) {
        cancelled |= cancel;
        if (stopped) return;
        stopped = true;
        retries.shutdownNow();
        if (channel != null) { channel.shutdownNow(); channel = null; }
        ended.accept(this);
    }
    @Override public void close() { stop(true); }
}
