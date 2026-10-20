package com.cs324.failover;

import com.cs324.common.WorkerEvent;
import com.cs324.failover.proto.CheckpointEntry;
import com.cs324.failover.proto.FetchReply;
import com.cs324.registry.proto.WorkerEndpoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

// Merge peer replicas, then recover jobs whose owners are gone without recomputing completed slices.
public final class FailoverCoordinator {

    // Hands a rebuilt job back to the job service. False when the job is already running here.
    public interface JobRestorer {
        boolean restore(RestoredJob job, long remainingMillis);
    }

    // Sends an already-computed answer to the client that is still waiting for it.
    public interface ResultDeliverer {
        boolean deliver(RestoredJob job);
    }

    private final int selfId;
    private final CheckpointLog log;
    private final CheckpointSource peers;
    private final JobRestorer restorer;
    private final ResultDeliverer deliverer;
    private final LongSupplier currentTerm;
    private final long jobTimeoutMillis;
    private final Consumer<String> events;
    // One takeover per job and term, so repeated triggers cannot rebuild a job twice.
    private final Set<String> takenOver = new HashSet<>();

    public FailoverCoordinator(int selfId, CheckpointLog log, CheckpointSource peers,
            JobRestorer restorer, ResultDeliverer deliverer, LongSupplier currentTerm,
            long jobTimeoutMillis, Consumer<String> events) {
        this.selfId = selfId;
        this.log = log;
        this.peers = peers;
        this.restorer = restorer;
        this.deliverer = deliverer;
        this.currentTerm = currentTerm;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.events = events;
    }

    // Merge reachable replicas and return the number of jobs rebuilt.
    public synchronized int takeOver() {
        long term = Math.max(currentTerm.getAsLong(), 1L);
        events.accept(WorkerEvent.FAILOVER_STARTED + ": merging replicated job state at term " + term);
        Set<Integer> live = merge();
        int recovered = 0;
        for (int jobId : log.jobIds()) {
            if (rebuild(jobId, term, live)) {
                recovered++;
            }
        }
        events.accept(WorkerEvent.FAILOVER_COMPLETED + ": merged " + live.size() + " answering peer(s), recovered "
                + recovered + " job(s) at term " + term);
        return recovered;
    }

    // Merge responding replicas and return their worker IDs; directory membership alone does not prove liveness.
    private Set<Integer> merge() {
        Set<Integer> live = new HashSet<>();
        for (WorkerEndpoint peer : peers.readyPeers()) {
            FetchReply reply = peers.fetch(peer);
            if (reply == null) {
                events.accept("Worker " + peer.getWorkerId() + " did not answer; treating it as gone");
                continue;
            }
            live.add(peer.getWorkerId());
            for (CheckpointEntry entry : reply.getEntriesList()) {
                log.apply(entry);
            }
        }
        return live;
    }

    // Return true when this worker rebuilds the replicated job.
    private boolean rebuild(int jobId, long term, Set<Integer> live) {
        JobCheckpoint job = log.job(jobId);
        if (job == null) {
            return false;
        }
        // Allow only one takeover per job and term, including concurrent recovery attempts.
        if (!takenOver.add(jobId + "@" + term)) {
            return false;
        }
        if (job.finished()) {
            if (!job.delivered()) {
                // Replay an undelivered result; the client ignores duplicate results for the same job.
                events.accept("job " + jobId + " finished before the failure without a confirmed"
                        + " client delivery; replaying the result");
                if (deliverer.deliver(toRestoredJob(job))) {
                    log.markDelivered(jobId, true);
                } else {
                    takenOver.remove(jobId + "@" + term);
                }
            }
            return false;
        }

        // Leave jobs with a live owner alone, even if leadership rotated to another worker.
        int owner = job.acceptedBy();
        if (owner != selfId && owner != -1 && live.contains(owner)) {
            return false;
        }

        List<RestoredJob.RestoredSlice> slices = new ArrayList<>(job.slices().size());
        for (JobCheckpoint.Slice slice : job.slices()) {
            slices.add(restoreSlice(slice, live));
        }
        RestoredJob restored = new RestoredJob(job.request(), slices, job.delivered());
        if (!restorer.restore(restored, remainingMillis(job))) {
            return false;
        }
        events.accept(WorkerEvent.JOB_RECOVERED + ": job " + jobId + " (" + job.request().getJobType()
                + ") resumed at term " + term + " with " + job.unfinished().size() + " unfinished slice(s)");
        return true;
    }

    // Keep completed or live-owner slices; retry orphaned slices with a higher attempt to reject stale results.
    private RestoredJob.RestoredSlice restoreSlice(JobCheckpoint.Slice slice, Set<Integer> live) {
        if (slice.completed()) {
            return new RestoredJob.RestoredSlice(slice.taskId(), slice.input(), slice.owner(),
                    slice.attempt(), slice.result());
        }
        int ownerId = slice.owner() == null ? 0 : slice.owner().getWorkerId();
        if (live.contains(ownerId)) {
            return new RestoredJob.RestoredSlice(slice.taskId(), slice.input(), slice.owner(),
                    slice.attempt(), null);
        }
        WorkerEndpoint self = WorkerEndpoint.newBuilder().setWorkerId(selfId).build();
        return new RestoredJob.RestoredSlice(slice.taskId(), slice.input(), self, slice.attempt() + 1, null);
    }

    private RestoredJob toRestoredJob(JobCheckpoint job) {
        List<RestoredJob.RestoredSlice> slices = new ArrayList<>(job.slices().size());
        for (JobCheckpoint.Slice slice : job.slices()) {
            slices.add(new RestoredJob.RestoredSlice(slice.taskId(), slice.input(), slice.owner(),
                    slice.attempt(), slice.result()));
        }
        return new RestoredJob(job.request(), slices, job.delivered(), job.finalResult());
    }

    // Use the remaining original deadline, with a minimum recovery window.
    private long remainingMillis(JobCheckpoint job) {
        long elapsed = System.currentTimeMillis() - job.acceptedAtMillis();
        return Math.max(jobTimeoutMillis - elapsed, com.cs324.common.Constants.FAILOVER_MIN_REMAINING_MS);
    }
}
