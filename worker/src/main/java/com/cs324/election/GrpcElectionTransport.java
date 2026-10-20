package com.cs324.election;

import com.cs324.computation.RegistryClient;
import com.cs324.election.proto.EchoMessage;
import com.cs324.election.proto.ElectionMessage;
import com.cs324.election.proto.CoordinatorAnnouncement;
import com.cs324.election.proto.ElectionServiceGrpc;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Election hop over gRPC.
 *
 * <p>Peer endpoints come from {@code RegistryService.GetWorkerAddress}, which
 * publishes a worker's {@code grpc_port}, so nothing here guesses a port.
 * Endpoints are resolved lazily and one
 * {@link ManagedChannel} is cached per peer, because a wave fans out along many
 * hops and reconnecting per hop would dominate the latency.
 *
 * <p>After a failed hop the caller is told the peer was not reached and removes
 * the edge. Which failures invalidate
 * the cached channel is deliberate - an {@code UNAVAILABLE} peer may have restarted
 * on a different port, so the entry is dropped and re-resolved, while a
 * {@code DEADLINE_EXCEEDED} peer is merely slow and its channel is still correct.
 */
public final class GrpcElectionTransport implements ElectionTransport {

    private record Peer(String host, int port, ManagedChannel channel) {}

    private final int selfId;
    private final RegistryClient directory;
    private final long deadlineMillis;
    private final Consumer<String> log;
    private final Map<Integer, Peer> peers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public GrpcElectionTransport(int selfId, RegistryClient directory, long deadlineMillis, Consumer<String> log) {
        this.selfId = selfId;
        this.directory = directory;
        this.deadlineMillis = deadlineMillis;
        this.log = log;
    }

    @Override
    public boolean sendElection(int peerId, int originId, int candidateId, int candidateJac, long messageId, long term,
            java.util.Set<Integer> voters) {
        if (peerId == selfId) {
            return false;
        }
        try {
            stubFor(peerId).sendElection(ElectionMessage.newBuilder()
                    .setOriginId(originId).setCandidateId(candidateId).setCandidateJac(candidateJac)
                    .setMessageId(messageId).setTerm(term).addAllVoters(voters).build());
            return true;
        } catch (StatusRuntimeException e) {
            return failed(peerId, "election hop", e);
        } catch (RuntimeException e) {
            // Discovery itself failed: no endpoint, so this peer is unreachable for now.
            return failed(peerId, "election hop", e);
        }
    }

    @Override
    public boolean sendEcho(int peerId, long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac,
            long term, java.util.Set<Integer> voters) {
        if (peerId == selfId) {
            return false;
        }
        try {
            stubFor(peerId).sendEcho(EchoMessage.newBuilder()
                    .setMessageId(messageId).setFromWorkerId(fromWorkerId)
                    .setBestCandidateId(bestCandidateId).setBestCandidateJac(bestCandidateJac)
                    .setTerm(term).addAllVoters(voters).build());
            return true;
        } catch (StatusRuntimeException e) {
            return failed(peerId, "echo hop", e);
        } catch (RuntimeException e) {
            return failed(peerId, "echo hop", e);
        }
    }

    @Override
    public boolean sendAnnouncement(int peerId, int coordinatorId, long messageId, long term) {
        if (peerId == selfId) {
            return false;
        }
        try {
            stubFor(peerId).announceCoordinator(CoordinatorAnnouncement.newBuilder()
                    .setCoordinatorId(coordinatorId).setMessageId(messageId).setTerm(term).build());
            return true;
        } catch (StatusRuntimeException e) {
            return failed(peerId, "coordinator announcement", e);
        } catch (RuntimeException e) {
            return failed(peerId, "coordinator announcement", e);
        }
    }

    /** One failure funnel, so every hop logs the same way and invalidates the same way. */
    private boolean failed(int peerId, String what, Throwable error) {
        Status status = Status.fromThrowable(error);
        if (status.getCode() == Status.Code.UNAVAILABLE || status.getCode() == Status.Code.INTERNAL
                || status.getCode() == Status.Code.UNKNOWN || status.getCode() == Status.Code.CANCELLED) {
            drop(peerId, status.getCode().name());
        }
        log.accept("gRPC " + what + " to Worker " + peerId + " failed (" + status.getCode() + "): "
                + (status.getDescription() == null ? error.getMessage() : status.getDescription()));
        return false;
    }

    /** Cached channel per peer, resolved through the directory on first use. */
    private synchronized ElectionServiceGrpc.ElectionServiceBlockingStub stubFor(int peerId) {
        if (closed.get()) {
            throw new IllegalStateException("Election transport for Worker " + selfId + " is closed");
        }
        Peer cached = peers.get(peerId);
        if (cached == null) {
            WorkerEndpoint endpoint = directory.worker(peerId);
            if (endpoint.getHost().isBlank() || endpoint.getGrpcPort() < 1) {
                throw new IllegalStateException("Worker " + peerId + " has no usable gRPC endpoint");
            }
            Peer created = new Peer(endpoint.getHost(), endpoint.getGrpcPort(),
                    ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getGrpcPort())
                            .usePlaintext().build());
            Peer winner = peers.putIfAbsent(peerId, created);
            if (winner != null) {
                // Another hop resolved the same peer first; keep its channel, drop ours.
                created.channel().shutdownNow();
                cached = winner;
            } else {
                cached = created;
            }
        }
        return ElectionServiceGrpc.newBlockingStub(cached.channel())
                .withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS);
    }

    /** Forgets a peer so the next hop resolves its endpoint again after a restart. */
    private void drop(int peerId, String reason) {
        Peer removed = peers.remove(peerId);
        if (removed != null) {
            removed.channel().shutdownNow();
            log.accept("dropped cached gRPC channel for Worker " + peerId + " (" + reason + ")");
        }
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Peer peer : peers.values()) {
            peer.channel().shutdownNow();
        }
        peers.clear();
    }
}
