package com.cs324.computation;

import com.cs324.common.WorkerRemote;
import com.cs324.common.WorkerEvent;
import com.cs324.control.proto.*;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Local adapter for historical Java interfaces; every wire call uses protobuf/gRPC. */
public final class GrpcWorkerClient implements WorkerRemote {
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
    // These old Java-object entry points are never used by the application. Typed job
    // and election callers use JobService/ElectionService directly, preserving terms.
    private UnsupportedOperationException typed() { return new UnsupportedOperationException("Use the typed gRPC job/election service"); }
    @Override public void submitJob(int id, String type, Object payload, String host, int port, String name) { throw typed(); }
    @Override public void assignTask(int id, String type, Object payload) { throw typed(); }
    @Override public void reportTaskResult(int job, int worker, Object result) { throw typed(); }
    @Override public void receiveElectionMessage(int origin, int candidate, int jac, long message) { throw typed(); }
    @Override public void receiveEchoMessage(long message, int from, int candidate, int jac) { throw typed(); }
    @Override public void receiveCoordinatorMessage(int coordinator, long message) { throw typed(); }
}
