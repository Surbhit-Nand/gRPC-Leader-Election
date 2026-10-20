package com.cs324.computation;

import com.cs324.common.WorkerControl;
import com.cs324.common.WorkerEvent;
import com.cs324.control.proto.*;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Local adapter for historical Java interfaces; every wire call uses protobuf/gRPC. */
public final class GrpcWorkerClient implements WorkerControl {
    private final WorkerEndpoint endpoint;
    public GrpcWorkerClient(WorkerEndpoint endpoint) { this.endpoint = endpoint; }
    private <T> T call(Function<WorkerControlServiceGrpc.WorkerControlServiceBlockingStub, T> action) {
        var channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getGrpcPort()).usePlaintext().build();
        try { return action.apply(WorkerControlServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)); }
        finally { channel.shutdownNow(); }
    }
    public WorkerStatus status() { return call(s -> s.getStatus(StatusRequest.getDefaultInstance())); }
    @Override public int getId() { return endpoint.getWorkerId(); }
    @Override public int getProgressPort() { return endpoint.getGrpcPort(); }
    @Override public int getJobAllocationCounter() { return status().getJac(); }
    @Override public int getCurrentCoordinatorId() { return status().getCoordinatorId(); }
    @Override public Set<Integer> getNeighbourIds() { return Set.copyOf(status().getNeighbourIdsList()); }
    @Override public boolean isActive() { status(); return true; }
    @Override public void ping() { status(); }
    @Override public long startElection() { return call(s -> s.startElection(ControlAck.getDefaultInstance())).getMessageId(); }
    @Override public void connectAsNeighbour(int id, String host, int port) {
        call(s -> s.connectPeer(ConnectPeerRequest.newBuilder().setWorkerId(id).build()));
    }
    @Override public void notifyLeaving(int id, Set<Integer> neighbours) {
        call(s -> s.notifyLeaving(LeavingRequest.newBuilder().setWorkerId(id).addAllNeighbourIds(neighbours).build()));
    }
    @Override public List<WorkerEvent> getRecentEvents(long after) {
        return call(s -> s.getStatus(StatusRequest.newBuilder().setAfterSequence(after).build())).getEventsList().stream()
                .map(e -> new WorkerEvent(e.getSequence(), e.getWorkerId(), e.getEpochMillis(), e.getKind(), e.getDetail())).toList();
    }
}
