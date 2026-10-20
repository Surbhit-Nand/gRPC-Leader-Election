package com.cs324.failover;

import com.cs324.failover.proto.SliceState;
import com.cs324.failover.proto.SliceUpdated;
import com.cs324.job.proto.SubmitJobRequest;
import com.cs324.job.proto.TaskPayload;
import com.cs324.job.proto.TaskResult;
import com.cs324.registry.proto.WorkerEndpoint;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One job as a replica holds it: the original request, every slice with its input and
 * current owner, and the final result once the job ended.
 *
 * <p>The slice table is the whole point of the log. Holding the inputs means a survivor
 * rebuilds the exact same split, and holding the completed slices' results means recovery
 * never recomputes work that already finished - which is what keeps a failover from
 * double counting a PRIMESUM or PRIMECOUNT.
 *
 * <p>Not thread-safe on its own: {@link CheckpointLog} owns the locking, because the
 * sequence number that guards every mutation belongs to the log, not to a job.
 */
public final class JobCheckpoint {

    /** One slice: stable identity, the input it computes, who owns it, and any result. */
    public record Slice(int taskId, TaskPayload input, WorkerEndpoint owner, int attempt, TaskResult result) {

        /** True once this slice has produced its answer and must not be recomputed. */
        public boolean completed() {
            return result != null;
        }
    }

    private final SubmitJobRequest request;
    private final long acceptedAtMillis;
    private final Map<Integer, Slice> slices = new LinkedHashMap<>();
    private long sequence;
    private long term;
    // The coordinator that accepted this job. Job ownership is not leadership: after an
    // ordinary rotation the previous coordinator is alive and still finishing its jobs, and
    // only its death makes the job another worker's problem.
    private int acceptedBy = -1;
    private TaskResult finalResult;
    private boolean delivered;

    public JobCheckpoint(SubmitJobRequest request, long acceptedAtMillis, long sequence, long term) {
        if (request == null) {
            throw new IllegalArgumentException("A checkpoint needs the original job request");
        }
        if (request.getJobId() < 1) {
            throw new IllegalArgumentException("Positive job ID required");
        }
        this.request = request;
        this.acceptedAtMillis = acceptedAtMillis;
        this.sequence = sequence;
        this.term = term;
    }

    /** Worker id of the coordinator that accepted the job, or -1 when unknown. */
    public int acceptedBy() {
        return acceptedBy;
    }

    /** Detached read view; called while the owning log holds its monitor. */
    JobCheckpoint copy() {
        JobCheckpoint copy = new JobCheckpoint(request, acceptedAtMillis, sequence, term);
        copy.slices.putAll(slices);
        copy.acceptedBy = acceptedBy;
        copy.finalResult = finalResult;
        copy.delivered = delivered;
        return copy;
    }

    void acceptedBy(int coordinatorId) {
        this.acceptedBy = coordinatorId;
    }

    public SubmitJobRequest request() {
        return request;
    }

    public int jobId() {
        return request.getJobId();
    }

    public long acceptedAtMillis() {
        return acceptedAtMillis;
    }

    /** Highest per-job sequence folded in so far; the base for the next entry. */
    public long sequence() {
        return sequence;
    }

    public long term() {
        return term;
    }

    /** A finished job has a result and no further transitions are expected. */
    public boolean finished() {
        return finalResult != null;
    }

    public TaskResult finalResult() {
        return finalResult;
    }

    /** Whether the client callback for the final result was acknowledged. */
    public boolean delivered() {
        return delivered;
    }

    public List<Slice> slices() {
        return List.copyOf(slices.values());
    }

    public Slice slice(int taskId) {
        return slices.get(taskId);
    }

    /** Slices with no result yet: the only work a takeover may have to arrange. */
    public List<Slice> unfinished() {
        List<Slice> pending = new ArrayList<>();
        for (Slice slice : slices.values()) {
            if (!slice.completed()) {
                pending.add(slice);
            }
        }
        return pending;
    }

    /** Installs the initial slice table. Only valid while the table is empty. */
    void adoptInitialSlices(List<SliceState> states) {
        if (!slices.isEmpty()) {
            throw new IllegalStateException("Job " + jobId() + " already has slices to replace");
        }
        for (SliceState state : states) {
            slices.put(state.getTaskId(), new Slice(state.getTaskId(), state.getInput(),
                    state.getOwner(), Math.max(state.getAttempt(), 1), state.hasResult() ? state.getResult() : null));
        }
    }

    /** Replaces the whole table, used only by a FetchCheckpoints snapshot. */
    void replaceSlices(List<SliceState> states) {
        slices.clear();
        adoptInitialSlices(states);
    }

    /**
     * Applies one delta. Absent fields mean unchanged, so an update can carry an owner
     * change, an attempt change, a result, or any combination.
     */
    void applySliceDelta(SliceUpdated update) {
        Slice current = slices.get(update.getTaskId());
        if (current == null) {
            // An update for a slice this replica never saw accepted: keep the log's shape
            // rather than inventing an input we do not have.
            return;
        }
        WorkerEndpoint owner = update.getOwner().getWorkerId() > 0 ? update.getOwner() : current.owner();
        int attempt = update.getAttempt() > 0 ? update.getAttempt() : current.attempt();
        TaskResult result = update.hasResult() ? update.getResult() : current.result();
        slices.put(current.taskId(), new Slice(current.taskId(), current.input(), owner, attempt, result));
    }

    void markFinished(TaskResult result, boolean wasDelivered) {
        this.finalResult = result;
        this.delivered = wasDelivered;
    }

    void markDelivered(boolean value) {
        this.delivered = value;
    }

    void advance(long newSequence, long newTerm) {
        this.sequence = newSequence;
        this.term = newTerm;
    }

    /** Current state as a snapshot slice list, for serving a peer that asks for it. */
    public List<SliceState> snapshotSlices() {
        List<SliceState> states = new ArrayList<>(slices.size());
        for (Slice slice : slices.values()) {
            SliceState.Builder state = SliceState.newBuilder()
                    .setTaskId(slice.taskId()).setInput(slice.input())
                    .setOwner(slice.owner()).setAttempt(slice.attempt());
            if (slice.result() != null) {
                state.setResult(slice.result());
            }
            states.add(state.build());
        }
        return states;
    }
}
