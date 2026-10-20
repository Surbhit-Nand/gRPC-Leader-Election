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

/**
 * Ships this coordinator's log entries to every ready peer, and reads theirs back when a
 * takeover needs one.
 *
 * <p>Every published peer is offered a cumulative snapshot. Acceptance requires at least one
 * peer to acknowledge its sequence within the deadline. A peerless cluster refuses new jobs.
 * Slice updates and completion remain asynchronous; takeover can recompute unfinished work.
 *
 * <p>One channel per peer is cached, as in the election transport, and every call carries a
 * deadline so a silent peer can never block the coordinator's own thread.
 */
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

    /**
     * Puts one entry on every ready peer.
     *
     * <p>Every peer is always asked: a replica that missed the acceptance could never fold a
     * later delta (a delta carries no input), so a partial push would silently remove that peer
     * from the set of workers that can take over this job.
     *
     * <p>{@code waitForAck} is used for acceptance, where the client's acknowledgement depends
     * on the entry having landed somewhere. It waits for the <em>first</em> acknowledgement up to
     * a deadline rather than for all of them, so one slow peer cannot hold up a submission; the
     * remaining peers are still being written to in the background.
     *
     * @return for acceptance, true only after a peer confirms the requested sequence;
     *         for asynchronous updates, true when pushes were scheduled.
     */
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

    /** Asynchronous single-peer push; failures are logged and dropped on purpose. */
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

    /**
     * Reads one peer's log. Called during takeover for every reachable worker; a peer that
     * cannot answer is simply one fewer source, never a failed takeover.
     */
    @Override
    /**
     * Reads one peer's log. Called during takeover for every reachable worker; a peer that
     * cannot answer is one fewer source, never a failed takeover - and it is also how the
     * takeover learns that the peer is gone.
     *
     * @return the peer's entries, or null when the peer did not answer.
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

    /** Cached channel per peer; a peer that changed port gets a fresh one on the next call. */
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
