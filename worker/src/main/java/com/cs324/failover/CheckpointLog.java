package com.cs324.failover;

import com.cs324.common.Constants;
import com.cs324.failover.proto.CheckpointEntry;
import com.cs324.failover.proto.JobAccepted;
import com.cs324.failover.proto.JobFinished;
import com.cs324.failover.proto.SliceState;
import com.cs324.failover.proto.SliceUpdated;
import com.cs324.job.proto.SubmitJobRequest;
import com.cs324.job.proto.TaskResult;
import com.cs324.registry.proto.WorkerEndpoint;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Fold job checkpoints only when their sequence advances and their term is not older.
public final class CheckpointLog {

    private final int replicaId;
    private final Map<Integer, JobCheckpoint> jobs = new ConcurrentHashMap<>();
    // Finished jobs leave the replica eventually, oldest first, so memory stays bounded.
    private final Deque<Integer> finishedOrder = new ArrayDeque<>();

    public CheckpointLog(int replicaId) {
        this.replicaId = replicaId;
    }

    public int replicaId() {
        return replicaId;
    }

    // Return true only when the entry advances the replica state.
    public synchronized boolean apply(CheckpointEntry entry) {
        int jobId = entry.getJobId();
        if (jobId < 1 || entry.getEntryCase() == CheckpointEntry.EntryCase.ENTRY_NOT_SET) {
            return false;
        }
        JobCheckpoint job = jobs.get(jobId);
        if (job != null) {
            // Generation first: a deposed coordinator must not resurrect a term it has lost.
            if (entry.getTerm() < job.term()) {
                return false;
            }
            // Sequence decides within a term and carries across terms, so a resumed sequence space can never rewind and replay an old transition.
            if (entry.getSequence() <= job.sequence()) {
                return false;
            }
        } else if (entry.getEntryCase() != CheckpointEntry.EntryCase.ACCEPTED) {
            // A delta or a tombstone for a job this replica never saw accepted carries no input, so nothing here could be rebuilt from it.
            return false;
        }
        if (job == null) {
            JobAccepted accepted = entry.getAccepted();
            if (!accepted.hasRequest()) {
                return false;
            }
            job = new JobCheckpoint(accepted.getRequest(), accepted.getAcceptedAtMillis(),
                    entry.getSequence(), entry.getTerm());
            job.adoptInitialSlices(accepted.getSlicesList());
            job.acceptedBy(entry.getCoordinatorId());
            jobs.put(jobId, job);
        } else {
            switch (entry.getEntryCase()) {
                // A snapshot replaces the table wholesale, which is how FetchCheckpoints hands over a job whose slices moved on since acceptance.
                case ACCEPTED -> job.replaceSlices(entry.getAccepted().getSlicesList());
                case SLICE -> job.applySliceDelta(entry.getSlice());
                case FINISHED -> job.markFinished(entry.getFinished().getResult(),
                        entry.getFinished().getDelivered());
                default -> { }
            }
        }
        job.advance(entry.getSequence(), Math.max(entry.getTerm(), job.term()));
        retain(job);
        return true;
    }

    // Every job this replica can still rebuild: unfinished, or finished but undelivered.
    public synchronized Collection<JobCheckpoint> recoverable() {
        List<JobCheckpoint> recoverable = new ArrayList<>();
        for (JobCheckpoint job : jobs.values()) {
            if (!job.finished() || !job.delivered()) {
                recoverable.add(job.copy());
            }
        }
        return recoverable;
    }

    public synchronized JobCheckpoint job(int jobId) {
        JobCheckpoint job = jobs.get(jobId);
        return job == null ? null : job.copy();
    }

    public int size() {
        return jobs.size();
    }

    // Next sequence for a job this replica coordinates: strictly above everything folded in.
    public synchronized long nextSequence(int jobId) {
        JobCheckpoint job = jobs.get(jobId);
        return job == null ? 1L : job.sequence() + 1;
    }

    // Builds the entry that records a job acceptance.
    public CheckpointEntry acceptedEntry(SubmitJobRequest request, List<CheckpointSink.SliceSnapshot> slices,
            long acceptedAtMillis, long term, int coordinatorId) {
        JobAccepted.Builder accepted = JobAccepted.newBuilder()
                .setRequest(request)
                .setAcceptedAtMillis(acceptedAtMillis);
        for (CheckpointSink.SliceSnapshot slice : slices) {
            accepted.addSlices(SliceState.newBuilder()
                    .setTaskId(slice.taskId()).setInput(slice.input())
                    .setOwner(slice.owner()).setAttempt(slice.attempt()));
        }
        return CheckpointEntry.newBuilder()
                .setJobId(request.getJobId())
                .setSequence(nextSequence(request.getJobId()))
                .setTerm(term)
                .setCoordinatorId(coordinatorId)
                .setAccepted(accepted)
                .build();
    }

    // Builds a slice delta. Unset fields mean "unchanged" on the receiving replica.
    public CheckpointEntry sliceEntry(int jobId, int taskId, WorkerEndpoint owner, int attempt,
            TaskResult result, long term, int coordinatorId) {
        SliceUpdated.Builder update = SliceUpdated.newBuilder().setTaskId(taskId);
        if (owner != null) {
            update.setOwner(owner);
        }
        if (attempt > 0) {
            update.setAttempt(attempt);
        }
        if (result != null) {
            update.setResult(result);
        }
        return CheckpointEntry.newBuilder()
                .setJobId(jobId)
                .setSequence(nextSequence(jobId))
                .setTerm(term)
                .setCoordinatorId(coordinatorId)
                .setSlice(update)
                .build();
    }

    // Builds the tombstone that stops a finished job from being rebuilt.
    public CheckpointEntry finishedEntry(int jobId, TaskResult result, boolean delivered,
            long term, int coordinatorId) {
        return CheckpointEntry.newBuilder()
                .setJobId(jobId)
                .setSequence(nextSequence(jobId))
                .setTerm(term)
                .setCoordinatorId(coordinatorId)
                .setFinished(JobFinished.newBuilder().setResult(result).setDelivered(delivered))
                .build();
    }

    // Export current slices and any terminal marker while preserving the job owner.
    public synchronized List<CheckpointEntry> snapshot(int jobId) {
        JobCheckpoint job = jobs.get(jobId);
        if (job == null) {
            return List.of();
        }
        List<CheckpointEntry> entries = new ArrayList<>(2);
        entries.add(CheckpointEntry.newBuilder()
                .setJobId(jobId)
                // A terminal snapshot needs two entries, but neither may invent a future sequence or the next real delivery update will be dropped.
                .setSequence(job.finished() ? job.sequence() - 1 : job.sequence())
                .setTerm(job.term())
                .setCoordinatorId(job.acceptedBy())
                .setAccepted(JobAccepted.newBuilder()
                        .setRequest(job.request())
                        .setAcceptedAtMillis(job.acceptedAtMillis())
                        .addAllSlices(job.snapshotSlices()))
                .build());
        if (job.finished()) {
            entries.add(CheckpointEntry.newBuilder()
                    .setJobId(jobId)
                    .setSequence(job.sequence())
                    .setTerm(job.term())
                    .setCoordinatorId(job.acceptedBy())
                    .setFinished(JobFinished.newBuilder()
                            .setResult(job.finalResult())
                            .setDelivered(job.delivered()))
                    .build());
        }
        return entries;
    }

    // Marks a job's client callback as acknowledged, so a later takeover will not replay it.
    public synchronized void markDelivered(int jobId, boolean delivered) {
        JobCheckpoint job = jobs.get(jobId);
        if (job != null) {
            job.markDelivered(delivered);
        }
    }

    // Job ids in ascending order, so a takeover walks the log deterministically.
    public List<Integer> jobIds() {
        List<Integer> ids = new ArrayList<>(jobs.keySet());
        ids.sort(Comparator.naturalOrder());
        return ids;
    }

    // Drops the oldest finished jobs once retention is exceeded.
    private void retain(JobCheckpoint job) {
        if (!job.finished()) {
            return;
        }
        if (!finishedOrder.contains(job.jobId())) {
            finishedOrder.addLast(job.jobId());
        }
        while (finishedOrder.size() > Constants.FAILOVER_CHECKPOINT_RETENTION) {
            Integer oldest = finishedOrder.pollFirst();
            if (oldest != null) {
                jobs.remove(oldest);
            }
        }
    }
}
