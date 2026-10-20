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

    /**
     * Merges every reachable replica and rebuilds the jobs this worker must take over.
     *
     * @return the number of jobs rebuilt in this pass.
     */
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

    /**
     * Reads every peer's replica into the local log, and learns which peers are alive on the way.
     *
     * <p>A crashed worker is still registered, so "listed in the directory" is not evidence that
     * it exists. A peer that does not answer the fetch is treated as gone, which is the same
     * signal the heartbeat produced and the same answer the decisions below need.
     *
     * @return the ids of the peers that answered.
     */
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

    /**
     * Rebuilds one replicated job if this worker is the one that must.
     *
     * @return true when the job was rebuilt here.
     */
    private boolean rebuild(int jobId, long term, Set<Integer> live) {
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
                if (deliverer.deliver(toRestoredJob(job))) {
                    log.markDelivered(jobId, true);
                } else {
                    takenOver.remove(jobId + "@" + term);
                }
            }
            return false;
        }

        // The job is only ours to take if the coordinator that accepted it is gone. A rotation
        // leaves the previous coordinator alive and still finishing its work; stealing that job
        // would run it twice and, for a sum, count it twice.
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

    /**
     * Chooses what happens to one slice.
     *
     * <p>Kept with a live owner: the worker still holds the work and answers the probe. Given
     * to the new coordinator with a higher attempt: the previous owner cannot answer, so the
     * slice is recomputed under a fresh attempt, which also fences any late answer from it.
     */
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

    /**
     * The deadline a restored job gets: what is left of its original one, with a floor so a
     * job that was nearly out of time still has a fair chance after the failover.
     */
    private long remainingMillis(JobCheckpoint job) {
        long elapsed = System.currentTimeMillis() - job.acceptedAtMillis();
        return Math.max(jobTimeoutMillis - elapsed, com.cs324.common.Constants.FAILOVER_MIN_REMAINING_MS);
    }
}
