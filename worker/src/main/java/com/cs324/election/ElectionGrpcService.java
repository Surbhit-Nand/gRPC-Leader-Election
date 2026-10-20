package com.cs324.election;

import com.cs324.election.proto.CoordinatorAck;
import com.cs324.election.proto.CoordinatorAnnouncement;
import com.cs324.election.proto.EchoAck;
import com.cs324.election.proto.EchoMessage;
import com.cs324.election.proto.ElectionAck;
import com.cs324.election.proto.ElectionMessage;
import com.cs324.election.proto.ElectionServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

// Translate election RPCs into worker calls and report handler failures as gRPC errors.
public final class ElectionGrpcService extends ElectionServiceGrpc.ElectionServiceImplBase {

    private final ElectionHandler handler;

    public ElectionGrpcService(ElectionHandler handler) {
        this.handler = handler;
    }

    @Override
    public void sendElection(ElectionMessage request, StreamObserver<ElectionAck> response) {
        if (request.getOriginId() < 1 || request.getCandidateId() < 1) {
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription("origin_id and candidate_id are required").asRuntimeException());
            return;
        }
        if (deliver(response, () -> handler.onElectionMessage(request))) {
            response.onNext(ElectionAck.getDefaultInstance());
            response.onCompleted();
        }
    }

    @Override
    public void sendEcho(EchoMessage request, StreamObserver<EchoAck> response) {
        if (request.getFromWorkerId() < 1) {
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription("from_worker_id is required").asRuntimeException());
            return;
        }
        if (deliver(response, () -> handler.onEchoMessage(request))) {
            response.onNext(EchoAck.getDefaultInstance());
            response.onCompleted();
        }
    }

    @Override
    public void announceCoordinator(CoordinatorAnnouncement request, StreamObserver<CoordinatorAck> response) {
        if (request.getCoordinatorId() < 1) {
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription("coordinator_id is required").asRuntimeException());
            return;
        }
        if (deliver(response, () -> handler.onCoordinatorAnnouncement(request))) {
            response.onNext(CoordinatorAck.getDefaultInstance());
            response.onCompleted();
        }
    }

    // Runs the handler and answers with an error status if it fails, so the caller can drop the edge.
    private boolean deliver(StreamObserver<?> response, Runnable action) {
        try {
            action.run();
            return true;
        } catch (RuntimeException e) {
            response.onError(Status.UNAVAILABLE.withDescription("Election hop could not be applied")
                    .withCause(e).asRuntimeException());
            return false;
        }
    }
}
