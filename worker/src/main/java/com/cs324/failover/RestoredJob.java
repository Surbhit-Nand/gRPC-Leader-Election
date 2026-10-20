package com.cs324.failover;

import com.cs324.job.proto.SubmitJobRequest;
import com.cs324.job.proto.TaskPayload;
import com.cs324.job.proto.TaskResult;
import com.cs324.registry.proto.WorkerEndpoint;

import java.util.List;

// Transfer recovered inputs and completed results to the job service without recomputing finished slices.
public record RestoredJob(SubmitJobRequest request, List<RestoredSlice> slices, boolean delivered,
        TaskResult finalResult) {

    public RestoredJob(SubmitJobRequest request, List<RestoredSlice> slices, boolean delivered) {
        this(request, slices, delivered, null);
    }

    public RestoredJob {
        if (request == null) {
            throw new IllegalArgumentException("A restored job needs the original request");
        }
        slices = List.copyOf(slices);
    }

    public int jobId() {
        return request.getJobId();
    }

    // Retain slice identity, input, attempt, owner and any completed result during recovery.
    public record RestoredSlice(int taskId, TaskPayload payload, WorkerEndpoint owner, int attempt,
            TaskResult result) {

        public boolean completed() {
            return result != null;
        }
    }
}
