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
import java.util.function.IntSupplier;

/**
 * What a new coordinator does with the replicated log once it has won an election.
 *
 * <p>The sequence is: merge, then decide per job, then rebuild. The decision is the whole
 * difficulty of failover, and it is deliberately conservative:
 *
 * <ul>
 *   <li>A job whose owner is still alive is <em>not</em> touched. Job ownership is not
 *       leadership - after an ordinary five-job rotation the previous coordinator is alive and
 *       still finishing its work, and stealing that job would run it twice.</li>
 *   <li>A job with a finished tombstone is not rebuilt. Its answer is already computed.</li>
 *   <li>Only a job owned by a worker that is gone is rebuilt, and only after the merge, so
 *       the newest view of every slice wins.</li>
 * </ul>
 *
 * <p>Within a rebuilt job, slices that already produced a result keep it; slices owned by a
 * live worker are re-probed against the new coordinator; slices whose owner is gone are
 * attempted once more, so a stale answer from a previous owner is fenced rather than counted.
 */
public final class FailoverCoordinator {

    /** Hands a rebuilt job back to the job service. False when the job is already running here. */
    public interface JobRestorer {
        boolean restore(RestoredJob job, long remainingMillis);
    }

    /** Sends an already-computed answer to the client that is still waiting for it. */
    public interface ResultDeliverer {
        boolean deliver(RestoredJob job);
    }

    private final int selfId;
    private final CheckpointLog log;
    private final CheckpointReplicator replicator;
    private final JobRestorer restorer;
    private final ResultDeliverer deliverer;
    private final IntSupplier currentTerm;
    private final long jobTimeoutMillis;
    private final Consumer<String> events;
    // One takeover per job and term, so repeated triggers cannot rebuild a job twice.
    private final Set<String> takenOver = new HashSet<>();

    public FailoverCoordinator(int selfId, CheckpointLog log, CheckpointReplicator replicator,
            JobRestorer restorer, ResultDeliverer deliverer, IntSupplier currentTerm,
            long jobTimeoutMillis, Consumer<String> events) {
        this.selfId = selfId;
        this.log = log;
        this.replicator = replicator;
        this.restorer = restorer;
        this.deliverer = deliverer;
        this.currentTerm = currentTerm;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.events = events;
    }

    /**
     * Merges every reachable replica and rebuilds the jobs this worker must take over.
     *
     * @return the number of jobs rebuilt in this pass.
     */
    public int takeOver() {
        long term = Math.max(currentTerm.getAsInt(), 1);
        events.accept(WorkerEvent.FAILOVER_STARTED + ": merging replicated job state at term " + term);
        int merged = merge();
        int recovered = 0;
        for (int jobId : log.jobIds()) {
            if (rebuild(jobId, term)) {
                recovered++;
            }
        }
        events.accept(WorkerEvent.FAILOVER_COMPLETED + ": merged " + merged + " checkpoint source(s), recovered "
                + recovered + " job(s) at term " + term);
        return recovered;
    }

    /** Reads every peer's replica into the local log; sequence and term rules do the merging. */
    private int merge() {
        int merged = 0;
        for (WorkerEndpoint peer : replicator.readyPeers()) {
            FetchReply reply = replicator.fetch(peer);
            for (CheckpointEntry entry : reply.getEntriesList()) {
                log.apply(entry);
            }
            if (!reply.getEntriesList().isEmpty()) {
                merged++;
            }
        }
        return merged;
    }

    /**
     * Rebuilds one replicated job if this worker is the one that must.
     *
     * @return true when the job was rebuilt here.
     */
    private boolean rebuild(int jobId, long term) {
        JobCheckpoint job = log.job(jobId);
        if (job == null) {
            return false;
        }
        // Idempotence gate: one takeover per job and term, so a repeated trigger, a second
        // election in the same term, or two threads racing here cannot rebuild it twice.
        if (!takenOver.add(jobId + "@" + term)) {
            return false;
        }
        if (job.finished()) {
            if (!job.delivered()) {
                // The answer exists but the client may never have been told. Replaying it is
                // safe: the client receiver already ignores a second result for the same job.
                events.accept("job " + jobId + " finished before the failure without a confirmed"
                        + " client delivery; replaying the result");
                deliverer.deliver(toRestoredJob(job));
                log.markDelivered(jobId, true);
            }
            return false;
        }

        List<WorkerEndpoint> live = replicator.readyPeers();
        List<RestoredJob.RestoredSlice> slices = new ArrayList<>(job.slices().size());
        for (JobCheckpoint.Slice slice : job.slices()) {
            slices.add(restoreSlice(slice, live, term));
        }
        RestoredJob restored = toRestoredJob(job, slices);
        if (!restorer.restore(restored, remainingMillis(job))) {
            return false;
        }
        events.accept(WorkerEvent.JOB_RECOVERED + ": job " + jobId + " (" + job.request().getJobType()
                + ") resumed at term " + term + " with " + job.unfinished().size() + " unfinished slice(s)");
        return true;
    }

    /**
     * Chooses what happens to one slice.
     *
     * <p>Kept with a live owner: the worker still holds the work and answers the probe. Given
     * to the new coordinator with a higher attempt: the previous owner cannot answer, so the
     * slice is recomputed under a fresh attempt, which also fences any late answer from it.
     */
    private RestoredJob.RestoredSlice restoreSlice(JobCheckpoint.Slice slice, List<WorkerEndpoint> live, long term) {
        if (slice.completed()) {
            return new RestoredJob.RestoredSlice(slice.taskId(), slice.input(), slice.owner(),
                    slice.attempt(), slice.result());
        }
        int ownerId = slice.owner() == null ? 0 : slice.owner().getWorkerId();
        boolean ownerAlive = live.stream().anyMatch(peer -> peer.getWorkerId() == ownerId);
        if (ownerAlive) {
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
        return toRestoredJob(job, slices);
    }

    private static RestoredJob toRestoredJob(JobCheckpoint job, List<RestoredJob.RestoredSlice> slices) {
        return new RestoredJob(job.request(), slices, job.delivered());
    }

    /**
     * The deadline a restored job gets: what is left of its original one, with a floor so a
     * job that was nearly out of time still has a fair chance after the failover.
     */
    private long remainingMillis(JobCheckpoint job) {
        long elapsed = System.currentTimeMillis() - job.acceptedAtMillis();
        return Math.max(jobTimeoutMillis - elapsed, com.cs324.common.Constants.FAILOVER_MIN_REMAINING_MS);
    }
}