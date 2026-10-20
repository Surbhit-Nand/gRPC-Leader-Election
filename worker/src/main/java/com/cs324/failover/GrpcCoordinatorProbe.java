package com.cs324.failover;

import com.cs324.common.Constants;
import com.cs324.computation.RegistryClient;
import com.cs324.failover.proto.FailoverServiceGrpc;
import com.cs324.failover.proto.HeartbeatReply;
import com.cs324.failover.proto.HeartbeatRequest;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Sends {@code FailoverService.Heartbeat} to one coordinator over gRPC.
 *
 * <p>A probe that cannot even be sent counts as a miss, not as an absence of information: a
 * coordinator this worker cannot resolve or connect to is exactly the failure the monitor
 * exists to notice. Silently ignoring that case is what let a dead coordinator stay
 * unnoticed before.
 */
public final class GrpcCoordinatorProbe implements CoordinatorHealthMonitor.Probe {

    private record Peer(String host, int port, ManagedChannel channel) {}

    private final int selfId;
    private final RegistryClient directory;
    private final LongSupplier currentTerm;
    private final IntSupplier believedCoordinator;
    private final Consumer<String> log;
    private final Map<Integer, Peer> peers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public GrpcCoordinatorProbe(int selfId, RegistryClient directory, LongSupplier currentTerm,
            IntSupplier believedCoordinator, Consumer<String> log) {
        this.selfId = selfId;
        this.directory = directory;
        this.currentTerm = currentTerm;
        this.believedCoordinator = believedCoordinator;
        this.log = log;
    }

    @Override
    public CoordinatorHealthMonitor.ProbeResult send(int coordinatorId) {
        if (closed.get()) {
            return null;
        }
        long started = System.nanoTime();
        try {
            WorkerEndpoint endpoint = directory.worker(coordinatorId);
            HeartbeatReply reply = FailoverServiceGrpc
                    .newBlockingStub(channelFor(endpoint))
                    .withDeadlineAfter(Constants.FAILOVER_HEARTBEAT_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .heartbeat(HeartbeatRequest.newBuilder()
                            .setWorkerId(selfId)
                            .setTerm(currentTerm.getAsLong())
                            .setCoordinatorId(believedCoordinator.getAsInt())
                            .build());
            return new CoordinatorHealthMonitor.ProbeResult((System.nanoTime() - started) / 1_000_000L,
                    reply.getTerm(), reply.getCoordinatorId(), reply.getIsCoordinator());
        } catch (StatusRuntimeException e) {
            log.accept("heartbeat to Worker " + coordinatorId + " failed (" + e.getStatus().getCode() + "): "
                    + e.getMessage());
            return null;
        } catch (RuntimeException e) {
            // Includes "cannot resolve this worker": indistinguishable from unreachable, and
            // both mean the coordinator cannot be counted on right now.
            log.accept("heartbeat to Worker " + coordinatorId + " could not be sent: " + e.getMessage());
            return null;
        }
    }

    private synchronized ManagedChannel channelFor(WorkerEndpoint endpoint) {
        if (closed.get()) throw new IllegalStateException("Coordinator probe is closed");
        Peer cached = peers.get(endpoint.getWorkerId());
        if (cached != null) {
            return cached.channel;
        }
        Peer created = new Peer(endpoint.getHost(), endpoint.getGrpcPort(),
                ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getGrpcPort())
                        .usePlaintext().build());
        Peer winner = peers.putIfAbsent(endpoint.getWorkerId(), created);
        if (winner != null) {
            created.channel().shutdownNow();
            return winner.channel;
        }
        return created.channel;
    }

    /** Releases cached channels; the probe cannot be used afterwards. */
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            for (Peer peer : peers.values()) {
                peer.channel().shutdownNow();
            }
            peers.clear();
        }
    }
}
