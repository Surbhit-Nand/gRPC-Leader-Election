package com.cs324.failover;

import com.cs324.job.proto.SubmitJobRequest;
import com.cs324.job.proto.TaskPayload;
import com.cs324.job.proto.TaskResult;
import com.cs324.registry.proto.WorkerEndpoint;

import java.util.List;

// Replication hooks: acceptance may reject the job, while later updates are best effort.
public interface CheckpointSink {

    // A job was accepted and split. The client is acknowledged after this returns.
    default void onAccepted(SubmitJobRequest request, List<SliceSnapshot> slices, long acceptedAtMillis) {}

    // One slice was dispatched, reassigned or completed. Unset fields mean "unchanged".
    default void onSliceUpdated(int jobId, int taskId, WorkerEndpoint owner, int attempt, TaskResult result) {}

    // The job reached a final result, so replicas must stop rebuilding it.
    default void onFinished(int jobId, TaskResult result) {}

    // The client callback was acknowledged, which closes the tombstone's only open question.
    default void onDelivered(int jobId) {}

    // The client callback failed. A survivor may replay it, so the tombstone records that.
    default void onDeliveryFailed(int jobId) {}

    // One slice as it was accepted: identity, input, owner and attempt.
    record SliceSnapshot(int taskId, TaskPayload input, WorkerEndpoint owner, int attempt) {}

    // Shared do-nothing sink for the path that runs without replication.
    static CheckpointSink noop() {
        return new CheckpointSink() {};
    }
}