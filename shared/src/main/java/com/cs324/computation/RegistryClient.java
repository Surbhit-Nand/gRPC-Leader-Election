package com.cs324.computation;

import com.cs324.registry.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** gRPC discovery client. The contact-port field is unused compatibility metadata. */
public final class RegistryClient implements AutoCloseable, com.cs324.common.ClusterDirectory {
    private final ManagedChannel channel;
    public RegistryClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }
    private RegistryServiceGrpc.RegistryServiceBlockingStub stub() {
        return RegistryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }
    // Publish the bound job/progress listener without allocating another worker identity.
    // The contact-port field is retained on the wire but is never used for communication.
    public void publish(int id, String host, int contactPort, int grpcPort) {
        stub().register(RegisterRequest.newBuilder().setWorkerId(id).setHost(host).setPort(contactPort)
                .setGrpcPort(grpcPort).setUpdateEndpoint(true).build());
    }
    // Use the shared bootstrap sequence to keep IDs distinct across clients.
    public int allocateJobId() {
        return stub().allocateJobId(AllocateJobIdRequest.getDefaultInstance()).getJobId();
    }
    @Override public List<Integer> getActiveWorkerIds() {
        return stub().getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance()).getWorkerIdsList();
    }
    @Override public List<Integer> getAllocatedWorkerIds() { return getActiveWorkerIds(); }
    @Override public com.cs324.common.WorkerAddress getWorkerAddress(int id) {
        var reply = stub().getWorkerAddress(GetWorkerAddressRequest.newBuilder().setWorkerId(id).build());
        if (!reply.getFound()) return null;
        var w = reply.getWorker();
        return new com.cs324.common.WorkerAddress(w.getHost(), w.getPort(), w.getGrpcPort());
    }
    @Override public void deregisterWorker(int id) { stub().deregister(DeregisterRequest.newBuilder().setWorkerId(id).build()); }
    @Override public com.cs324.common.WorkerRegistration registerWorkerAuto(String host, int port) {
        var reply = stub().register(RegisterRequest.newBuilder().setAutoAssign(true).setHost(host).setPort(port).build());
        return new com.cs324.common.WorkerRegistration(reply.getWorkerId(), reply.getHasPeer() ? new GrpcWorkerClient(reply.getPeer()) : null);
    }
    @Override public com.cs324.common.WorkerControl registerWorker(int id, String host, int port) {
        var reply = stub().register(RegisterRequest.newBuilder().setWorkerId(id).setHost(host).setPort(port).build());
        return reply.getHasPeer() ? new GrpcWorkerClient(reply.getPeer()) : null;
    }
    // Resolve a ready gRPC endpoint; only the published grpc_port is contacted.
    public WorkerEndpoint worker(int id) {
        GetWorkerAddressResponse reply = stub().getWorkerAddress(GetWorkerAddressRequest.newBuilder().setWorkerId(id).build());
        if (!reply.getFound() || reply.getWorker().getGrpcPort() < 1)
            throw new IllegalStateException("Worker " + id + " has no published gRPC job endpoint");
        return reply.getWorker();
    }
    // Refresh membership per job and skip workers that have not published a listener.
    public List<WorkerEndpoint> workers() {
        List<WorkerEndpoint> endpoints = new ArrayList<>();
        for (int id : stub().getActiveWorkers(GetActiveWorkersRequest.getDefaultInstance()).getWorkerIdsList()) {
            GetWorkerAddressResponse reply = stub().getWorkerAddress(GetWorkerAddressRequest.newBuilder().setWorkerId(id).build());
            if (reply.getFound() && reply.getWorker().getGrpcPort() > 0) endpoints.add(reply.getWorker());
        }
        return List.copyOf(endpoints);
    }
    @Override public void close() { channel.shutdownNow(); }
}
