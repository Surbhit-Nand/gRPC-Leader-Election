package com.cs324.progress;

import com.cs324.progress.proto.*;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.*;

/** Serializes snapshots and stream writes; retains the last 256 finished jobs. */
public final class ProgressService extends JobProgressServiceGrpc.JobProgressServiceImplBase {
    private final Map<Integer, JobProgress> snapshots = new LinkedHashMap<>();
    private final Map<Integer, List<ServerCallStreamObserver<JobProgress>>> watchers = new HashMap<>();

    public synchronized void begin(int jobId, Map<Integer, Long> units) {
        JobProgress.Builder snapshot = JobProgress.newBuilder().setJobId(jobId);
        units.forEach((id, total) -> snapshot.addWorkers(WorkerProgress.newBuilder()
                .setWorkerId(id).setTotalUnits(total).setStatus("Processing")));
        snapshots.put(jobId, snapshot.build());
    }

    public synchronized void complete(int jobId, int workerId, boolean success) {
        JobProgress current = snapshots.get(jobId);
        if (current == null) return;
        JobProgress.Builder next = current.toBuilder();
        for (int i = 0; i < next.getWorkersCount(); i++) {
            WorkerProgress worker = next.getWorkers(i);
            if (worker.getWorkerId() == workerId && worker.getStatus().equals("Processing")) {
                next.setWorkers(i, worker.toBuilder().setCompletedUnits(success ? worker.getTotalUnits() : 0)
                        .setStatus(success ? "Completed" : "Failed"));
            }
        }
        next.setFinished(next.getWorkersList().stream().noneMatch(w -> w.getStatus().equals("Processing")));
        JobProgress snapshot = next.build();
        snapshots.put(jobId, snapshot);
        List<ServerCallStreamObserver<JobProgress>> listeners = watchers.get(jobId);
        if (listeners != null) {
            listeners.removeIf(listener -> !send(listener, snapshot));
            if (snapshot.getFinished()) watchers.remove(jobId);
        }
        long count = snapshots.values().stream().filter(JobProgress::getFinished).count();
        Iterator<JobProgress> iterator = snapshots.values().iterator();
        while (count > 256 && iterator.hasNext()) {
            if (iterator.next().getFinished()) { iterator.remove(); count--; }
        }
    }

    private boolean send(ServerCallStreamObserver<JobProgress> observer, JobProgress snapshot) {
        if (observer.isCancelled()) return false;
        try {
            observer.onNext(snapshot);
            if (snapshot.getFinished()) observer.onCompleted();
            return !snapshot.getFinished();
        } catch (RuntimeException disconnected) { return false; }
    }

    @Override
    public synchronized void watchJob(WatchJobRequest request, StreamObserver<JobProgress> response) {
        int jobId = request.getJobId();
        JobProgress snapshot = snapshots.get(jobId);
        if (snapshot == null) {
            response.onError(Status.NOT_FOUND.withDescription("No progress retained for job " + jobId).asRuntimeException());
            return;
        }
        ServerCallStreamObserver<JobProgress> observer = (ServerCallStreamObserver<JobProgress>) response;
        observer.setOnCancelHandler(() -> {
            synchronized (ProgressService.this) {
                List<ServerCallStreamObserver<JobProgress>> listeners = watchers.get(jobId);
                if (listeners != null) {
                    listeners.remove(observer);
                    if (listeners.isEmpty()) watchers.remove(jobId);
                }
            }
        });
        if (send(observer, snapshot)) watchers.computeIfAbsent(jobId, key -> new ArrayList<>()).add(observer);
    }
}
