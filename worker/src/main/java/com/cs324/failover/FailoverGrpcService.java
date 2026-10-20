package com.cs324.failover;

import com.cs324.failover.proto.CheckpointEntry;
import com.cs324.failover.proto.FailoverServiceGrpc;
import com.cs324.failover.proto.FetchReply;
import com.cs324.failover.proto.FetchRequest;
import com.cs324.failover.proto.HeartbeatReply;
import com.cs324.failover.proto.HeartbeatRequest;
import com.cs324.failover.proto.ReplicateAck;
import com.cs324.failover.proto.ReplicateRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

/**
 * Server side of {@code FailoverService}.
 *
 * <p>All three RPCs are held by every worker, because any of them may be the survivor: a
 * follower answers probes about itself, and every worker keeps a replica that a later
 * coordinator can read. A handler failure is reported as an error status, so the caller
 * treats the peer as unreachable instead of assuming the entry was stored.
 */
public final class FailoverGrpcService extends FailoverServiceGrpc.FailoverServiceImplBase {

    private final FailoverHandler handler;
    private final int replicaId;

    public FailoverGrpcService(FailoverHandler handler, int replicaId) {
        this.handler = handler;
        this.replicaId = replicaId;
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatReply> response) {
        try {
            HeartbeatReply reply = handler.heartbeat(request.getWorkerId(), request.getTerm(),
                    request.getCoordinatorId());
            response.onNext(reply);
            response.onCompleted();
        } catch (RuntimeException e) {
            response.onError(Status.UNAVAILABLE.withDescription("Heartbeat could not be answered")
                    .withCause(e).asRuntimeException());
        }
    }

    @Override
    public void replicate(ReplicateRequest request, StreamObserver<ReplicateAck> response) {
        try {
            long acknowledged = handler.acceptEntries(request.getCoordinatorId(), request.getTerm(),
                    request.getEntriesList());
            response.onNext(ReplicateAck.newBuilder().setAcknowledgedSequence(acknowledged).build());
            response.onCompleted();
        } catch (RuntimeException e) {
            response.onError(Status.UNAVAILABLE.withDescription("Checkpoint entries could not be stored")
                    .withCause(e).asRuntimeException());
        }
    }

    @Override
    public void fetchCheckpoints(FetchRequest request, StreamObserver<FetchReply> response) {
        try {
            List<CheckpointEntry> entries = new ArrayList<>(handler.snapshots());
            response.onNext(FetchReply.newBuilder().setReplicaId(replicaId).addAllEntries(entries).build());
            response.onCompleted();
        } catch (RuntimeException e) {
            response.onError(Status.UNAVAILABLE.withDescription("Checkpoint log could not be read")
                    .withCause(e).asRuntimeException());
        }
    }
}