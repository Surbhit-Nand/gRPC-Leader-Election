package com.cs324.worker;

import com.cs324.computation.RegistryClient;
import com.cs324.control.proto.StatusRequest;
import com.cs324.control.proto.WorkerControlServiceGrpc;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** One bounded status probe per peer, used by the quorum monitor to test reachability. */
final class QuorumProbe implements AutoCloseable {

    private final RegistryClient directory;
    private final int deadlineMillis;
    private final Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    QuorumProbe(RegistryClient directory, int deadlineMillis) {
        this.directory = directory;
        this.deadlineMillis = deadlineMillis;
    }

    boolean reachable(int workerId) {
        if (closed.get()) {
            return false;
        }
        try {
            WorkerEndpoint endpoint = directory.worker(workerId);
            if (endpoint.getGrpcPort() < 1) {
                return false;
            }
            WorkerControlServiceGrpc.newBlockingStub(channelFor(endpoint))
                    .withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS)
                    .getStatus(StatusRequest.getDefaultInstance());
            return true;
        } catch (RuntimeException unreachableOrNotReady) {
            drop(workerId);
            return false;
        }
    }

    private ManagedChannel channelFor(WorkerEndpoint endpoint) {
        return channels.computeIfAbsent(endpoint.getWorkerId(),
                id -> ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getGrpcPort())
                        .usePlaintext().build());
    }

    private void drop(int workerId) {
        ManagedChannel channel = channels.remove(workerId);
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (ManagedChannel channel : channels.values()) {
                channel.shutdownNow();
            }
            channels.clear();
        }
    }
}
