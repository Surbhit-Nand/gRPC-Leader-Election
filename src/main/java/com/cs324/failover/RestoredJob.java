package com.cs324.failover;

import com.cs324.job.proto.SubmitJobRequest;
import com.cs324.job.proto.TaskPayload;
import com.cs324.job.proto.TaskResult;
import com.cs324.registry.proto.WorkerEndpoint;

import java.util.List;

/**
 * A replicated job handed back to the job service after a coordinator failure.
 *
 * <p>It is a plain record on purpose: the job service learns nothing about replication, it
 * simply receives a job in the state it should have been in. Slices that already have a
 * result come back with it, which is what keeps recovery from recomputing finished work -
 * and therefore from adding a PRIMESUM twice.
 */
public record RestoredJob(SubmitJobRequest request, List<RestoredSlice> slices, boolean delivered) {

    public RestoredJob {
        if (request == null) {
            throw new IllegalArgumentException("A restored job needs the original request");
        }
        slices = List.copyOf(slices);
    }

    public int jobId() {
        return request.getJobId();
    }

    /**
     * One slice as it will run again: identity, input, owner, attempt, and any result that
     * was already produced. An owner equal to the new coordinator means "compute it here".
     */
    public record RestoredSlice(int taskId, TaskPayload payload, WorkerEndpoint owner, int attempt,
            TaskResult result) {

        public boolean completed() {
            return result != null;
        }
    }
}