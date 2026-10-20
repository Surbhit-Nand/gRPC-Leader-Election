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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

// Replicate cumulative job snapshots to peers and fetch their state during takeover.
public final class CheckpointReplicator implements CheckpointSource, AutoCloseable {

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

    // Push to all ready peers; waitForAck requires one peer to confirm the sequence before the deadline.
    public boolean replicate(List<CheckpointEntry> entries, long term, boolean waitForAck) {
        if (entries.isEmpty()) return true;
        if (closed.get()) return false;
        List<WorkerEndpoint> targets = readyPeers();
        if (targets.isEmpty()) {
            log.accept("no reachable peer for checkpoint " + entries.get(0).getJobId()
                    + "; acceptance requires a replica");
            return false;
        }
        CountDownLatch acknowledged = new CountDownLatch(1);
        for (WorkerEndpoint target : targets) {
            try {
                executor.execute(() -> {
                    if (send(target, entries, term)) acknowledged.countDown();
                });
            } catch (java.util.concurrent.RejectedExecutionException stopped) {
                log.accept("checkpoint executor stopped");
            }
        }
        if (!waitForAck) {
            return true;
        }
        try {
            if (!acknowledged.await(Constants.FAILOVER_REPLICATION_DEADLINE_MS, TimeUnit.MILLISECONDS)) {
                log.accept("could not confirm checkpoint " + entries.get(0).getJobId()
                        + " on a peer before the deadline");
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    // Send a bounded RPC to one peer and return whether it confirmed the requested sequence.
    private boolean send(WorkerEndpoint target, List<CheckpointEntry> entries, long term) {
        return send(target, entries, term, Constants.FAILOVER_REPLICATION_RPC_DEADLINE_MS);
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
            return ack.getAcknowledgedSequence() >= entries.stream()
                    .mapToLong(CheckpointEntry::getSequence).max().orElse(0L);
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

    // Fetch a peer snapshot during takeover; unreachable peers contribute no state.
    @Override
    // Fetch a peer snapshot; null means unreachable, while an empty list means no retained jobs.
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
            log.accept("Worker " + target.getWorkerId() + " did not answer the checkpoint fetch ("
                    + e.getStatus().getCode() + "), treating it as gone: " + e.getMessage());
            return null;
        } catch (RuntimeException e) {
            log.accept("Worker " + target.getWorkerId() + " could not be asked for its checkpoints,"
                    + " treating it as gone: " + e.getMessage());
            return null;
        }
    }

    @Override
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

    // Cached channel per peer; a peer that changed port gets a fresh one on the next call.
    private synchronized ManagedChannel channelFor(WorkerEndpoint endpoint) {
        if (closed.get()) throw new IllegalStateException("Checkpoint replicator is closed");
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

    // Drops a peer's cached channel when it looks gone rather than merely slow.
    private void forget(int peerId, StatusRuntimeException error) {
        Status.Code code = error.getStatus().getCode();
        if (code == Status.Code.UNAVAILABLE || code == Status.Code.UNKNOWN || code == Status.Code.INTERNAL) {
            Peer removed = peers.remove(peerId);
            if (removed != null) {
                removed.channel().shutdownNow();
            }
        }
    }

    // Releases every cached channel; the coordinator cannot replicate after this.
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
