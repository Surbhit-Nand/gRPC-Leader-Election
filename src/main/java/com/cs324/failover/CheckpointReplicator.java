package com.cs324.failover;

import com.cs324.common.Constants;
import com.cs324.computation.RegistryClient;
import com.cs324.failover.proto.CheckpointEntry;
import com.cs324.failover.proto.FetchReply;
import com.cs324.failover.proto.FetchRequest;
import com.cs324.failover.proto.ReplicateAck;
import com.cs324.failover.proto.ReplicateRequest;
import com.cs324.failover.proto.FailoverServiceGrpc;
import com.cs324.registry.proto.WorkerEndpoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Ships this coordinator's log entries to every ready peer, and reads theirs back when a
 * takeover needs one.
 *
 * <p>The cluster is small, so replication is full rather than majority: every ready worker
 * keeps a copy of every accepted job. Slices and finishes are best effort and asynchronous,
 * because the coordinator can finish a job without any peer hearing the last delta; only
 * acceptance is waited for, and even that gives up after a bounded deadline so a cluster with
 * no reachable peer still runs instead of refusing work.
 *
 * <p>One channel per peer is cached, as in the election transport, and every call carries a
 * deadline so a silent peer can never block the coordinator's own thread.
 */
public final class CheckpointReplicator implements AutoCloseable {

    private record Peer(String host, int port, ManagedChannel channel) {}

    private final int selfId;
    private final RegistryClient directory;
    private final Executor executor;
    private final Consumer<String> log;
    private final Map<Integer, Peer> peers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public CheckpointReplicator(int selfId, RegistryClient directory, Executor executor, Consumer<String> log) {
        this.selfId = selfId;
        this.directory = directory;
        this.executor = executor;
        this.log = log;
    }

    /**
     * Puts one entry on every ready peer.
     *
     * @param waitForAck true for acceptance, where the caller needs the entry to have landed
     *                   on at least one peer before it acknowledges the client.
     * @return true when at least one peer stored the entry, or when no peer was reachable and
     *         the caller chose to carry on anyway.
     */
    public boolean replicate(List<CheckpointEntry> entries, long term, boolean waitForAck) {
        if (entries.isEmpty() || closed.get()) {
            return true;
        }
        List<WorkerEndpoint> targets = readyPeers();
        if (targets.isEmpty()) {
            log.accept("no reachable peer for checkpoint " + entries.get(0).getJobId()
                    + "; continuing without replication");
            return true;
        }
        if (!waitForAck) {
            for (WorkerEndpoint target : targets) {
                executor.execute(() -> send(target, entries, term));
            }
            return true;
        }
        // Bounded wait: one peer's answer is enough, and a late peer will catch up on the
        // deltas that follow.
        long deadline = System.currentTimeMillis() + Constants.FAILOVER_REPLICATION_DEADLINE_MS;
        for (WorkerEndpoint target : targets) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            if (send(target, entries, term, remaining)) {
                return true;
            }
        }
        log.accept("could not replicate checkpoint before the deadline; continuing unconfirmed");
        return true;
    }

    /** Asynchronous single-peer push; failures are logged and dropped on purpose. */
    private void send(WorkerEndpoint target, List<CheckpointEntry> entries, long term) {
        send(target, entries, term, Constants.FAILOVER_REPLICATION_RPC_DEADLINE_MS);
    }

    private boolean send(WorkerEndpoint target, List<CheckpointEntry> entries, long term, long timeoutMillis) {
        if (closed.get()) {
            return false;
        }
        try {
            ReplicateRequest request = ReplicateRequest.newBuilder()
                    .setCoordinatorId(selfId).setTerm(term).addAllEntries(entries).build();
            FailoverServiceGrpc.FailoverServiceBlockingStub stub = FailoverServiceGrpc
                    .newBlockingStub(channelFor(target))
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS);
            ReplicateAck ack = stub.replicate(request);
            log.accept("replicated " + entries.size() + " checkpoint entries to Worker "
                    + target.getWorkerId() + " (acknowledged through sequence " + ack.getAcknowledgedSequence() + ")");
            return true;
        } catch (StatusRuntimeException e) {
            forget(target.getWorkerId(), e);
            log.accept("checkpoint push to Worker " + target.getWorkerId() + " failed ("
                    + e.getStatus().getCode() + "): " + e.getMessage());
            return false;
        } catch (RuntimeException e) {
            log.accept("checkpoint push to Worker " + target.getWorkerId() + " could not be started: " + e.getMessage());
            return false;
        }
    }

    /**
     * Reads one peer's log. Called during takeover for every reachable worker; a peer that
     * cannot answer is simply one fewer source, never a failed takeover.
     */
    public FetchReply fetch(WorkerEndpoint target) {
        try {
            FetchReply reply = FailoverServiceGrpc.newBlockingStub(channelFor(target))
                    .withDeadlineAfter(Constants.FAILOVER_FETCH_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .fetchCheckpoints(FetchRequest.newBuilder().setRequesterId(selfId).build());
            log.accept("fetched " + reply.getEntriesCount() + " checkpoint entries from Worker "
                    + target.getWorkerId());
            return reply;
        } catch (StatusRuntimeException e) {
            forget(target.getWorkerId(), e);
            log.accept("fetch from Worker " + target.getWorkerId() + " failed ("
                    + e.getStatus().getCode() + "): " + e.getMessage());
            return FetchReply.getDefaultInstance();
        } catch (RuntimeException e) {
            log.accept("fetch from Worker " + target.getWorkerId() + " could not be started: " + e.getMessage());
            return FetchReply.getDefaultInstance();
        }
    }

    /** Every registered worker with a published gRPC endpoint, this one excluded. */
    public List<WorkerEndpoint> readyPeers() {
        List<WorkerEndpoint> ready = new ArrayList<>();
        try {
            for (WorkerEndpoint endpoint : directory.workers()) {
                if (endpoint.getWorkerId() != selfId && endpoint.getGrpcPort() > 0) {
                    ready.add(endpoint);
                }
            }
        } catch (RuntimeException e) {
            log.accept("discovery unavailable while replicating: " + e.getMessage());
        }
        return ready;
    }

    /** Cached channel per peer; a peer that changed port gets a fresh one on the next call. */
    private ManagedChannel channelFor(WorkerEndpoint endpoint) {
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

    /** Drops a peer's cached channel when it looks gone rather than merely slow. */
    private void forget(int peerId, StatusRuntimeException error) {
        Status.Code code = error.getStatus().getCode();
        if (code == Status.Code.UNAVAILABLE || code == Status.Code.UNKNOWN || code == Status.Code.INTERNAL) {
            Peer removed = peers.remove(peerId);
            if (removed != null) {
                removed.channel().shutdownNow();
            }
        }
    }

    /** Releases every cached channel; the coordinator cannot replicate after this. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Peer peer : peers.values()) {
            peer.channel().shutdownNow();
        }
        peers.clear();
    }
}