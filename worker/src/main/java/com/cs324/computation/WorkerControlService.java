package com.cs324.computation;

import com.cs324.control.proto.*;
import com.cs324.worker.WorkerRuntime;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;

/** gRPC facade over the existing topology and monitoring logic. */
public final class WorkerControlService extends WorkerControlServiceGrpc.WorkerControlServiceImplBase {
    private final WorkerRuntime worker;
    public WorkerControlService(WorkerRuntime worker) { this.worker = worker; }
    private interface Call<T> { T run() throws Exception; }
    private <T> void reply(StreamObserver<T> response, Call<T> call) {
        try { response.onNext(call.run()); response.onCompleted(); }
        catch (Exception e) { response.onError(Status.UNAVAILABLE.withDescription(e.getMessage()).withCause(e).asRuntimeException()); }
    }
    @Override public void getStatus(StatusRequest request, StreamObserver<WorkerStatus> response) {
        reply(response, () -> {
            var view = worker.heartbeat(0, 0, -1);
            var status = WorkerStatus.newBuilder().setWorkerId(worker.getId())
                    .setCoordinatorId(view.getCoordinatorId()).setTerm(view.getTerm())
                    .setJac(worker.getJobAllocationCounter()).setGrpcPort(worker.getProgressPort())
                    .addAllNeighbourIds(worker.getNeighbourIds());
            for (var event : worker.getRecentEvents(request.getAfterSequence()))
                status.addEvents(ClusterEvent.newBuilder().setSequence(event.seq()).setWorkerId(event.workerId())
                        .setEpochMillis(event.epochMillis()).setKind(event.kind()).setDetail(event.detail()));
            return status.build();
        });
    }
    @Override public void connectPeer(ConnectPeerRequest request, StreamObserver<ControlAck> response) {
        if (request.getWorkerId() < 1) { response.onError(Status.INVALID_ARGUMENT.asRuntimeException()); return; }
        reply(response, () -> { worker.connectGrpcNeighbour(request.getWorkerId()); return ControlAck.getDefaultInstance(); });
    }
    @Override public void notifyLeaving(LeavingRequest request, StreamObserver<ControlAck> response) {
        reply(response, () -> { worker.notifyLeaving(request.getWorkerId(), new HashSet<>(request.getNeighbourIdsList())); return ControlAck.getDefaultInstance(); });
    }
    @Override public void startElection(ControlAck request, StreamObserver<StartElectionReply> response) {
        reply(response, () -> StartElectionReply.newBuilder().setMessageId(worker.startElection()).build());
    }
}
