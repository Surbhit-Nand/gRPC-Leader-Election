package com.cs324.bootstrap;

import com.cs324.common.*;
import com.cs324.computation.GrpcWorkerClient;
import com.cs324.registry.proto.WorkerEndpoint;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory discovery directory. All network access is through RegistryService.
 *
 * <p>Membership entries carry a last-seen timestamp that is refreshed by registration and by
 * endpoint publication. {@link #expireIdle(long)} removes workers that stopped refreshing, so a
 * crashed process cannot remain in the directory (and inflate every quorum calculation) forever.
 */
public class BootstrapRegistryImpl implements ClusterDirectory {
    private final Map<Integer, WorkerAddress> activeWorkers = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastSeen = new ConcurrentHashMap<>();
    private final BootstrapStateStore stateStore;
    private final AtomicLong nextJobId;
    private final AtomicInteger nextWorkerId;

    public BootstrapRegistryImpl() {
        this(null);
    }

    public BootstrapRegistryImpl(BootstrapStateStore stateStore) {
        this.stateStore = stateStore;
        BootstrapStateStore.State state = stateStore == null
                ? BootstrapStateStore.State.initial()
                : stateStore.load();
        this.nextJobId = new AtomicLong(state.nextJobId());
        this.nextWorkerId = new AtomicInteger((int) Math.min(Integer.MAX_VALUE, state.nextWorkerId()));
    }

    @Override public synchronized int allocateJobId() {
        long id = nextJobId.getAndIncrement();
        if (id > Integer.MAX_VALUE) throw new IllegalStateException("Job IDs exhausted.");
        persistCounters();
        return (int) id;
    }

    @Override public WorkerControl registerWorker(int id, String host, int port) {
        return peerClient(registerGrpcRecord(id, false, host, port));
    }

    @Override public WorkerRegistration registerWorkerAuto(String host, int port) {
        var record = registerGrpcRecord(0, true, host, port);
        return new WorkerRegistration(record.workerId(), peerClient(record));
    }

    private WorkerControl peerClient(RegistrationRecord record) {
        if (record.peerId() == null) return null;
        return new GrpcWorkerClient(WorkerEndpoint.newBuilder().setWorkerId(record.peerId())
                .setHost(record.peerAddress().getHost())
                .setGrpcPort(record.peerAddress().getGrpcPort()).build());
    }

    record RegistrationRecord(int workerId, Integer peerId, WorkerAddress peerAddress) {}

    // No peer network calls under this lock. Joiners probe published endpoints with deadlines.
    synchronized RegistrationRecord registerGrpcRecord(int requestedId, boolean auto, String host, int port) {
        if (host.isBlank() || port < 0 || port > 65535 || (!auto && requestedId < 1))
            throw new IllegalArgumentException("Valid host, port and positive worker ID required");
        if (auto && nextWorkerId.get() >= Integer.MAX_VALUE)
            throw new IllegalStateException("Worker IDs exhausted.");
        int id = auto ? nextWorkerId.getAndIncrement() : requestedId;
        List<Map.Entry<Integer, WorkerAddress>> peers = new ArrayList<>(activeWorkers.entrySet());
        Collections.shuffle(peers);
        if (activeWorkers.putIfAbsent(id, new WorkerAddress(host, port)) != null)
            throw new DuplicateWorkerIdException(id);
        lastSeen.put(id, System.currentTimeMillis());
        nextWorkerId.updateAndGet(current -> id >= Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.max(current, id + 1));
        persistCounters();
        return peers.stream().filter(e -> e.getValue().getGrpcPort() > 0).findFirst()
                .map(e -> new RegistrationRecord(id, e.getKey(), e.getValue()))
                .orElse(new RegistrationRecord(id, null, null));
    }

    @Override public void deregisterWorker(int id) {
        activeWorkers.remove(id);
        lastSeen.remove(id);
    }
    @Override public List<Integer> getActiveWorkerIds() { return new ArrayList<>(activeWorkers.keySet()); }
    @Override public List<Integer> getAllocatedWorkerIds() { return getActiveWorkerIds(); }
    @Override public WorkerAddress getWorkerAddress(int id) { return activeWorkers.get(id); }

    void publishGrpcEndpoint(int id, String host, int contactPort, int grpcPort) {
        if (grpcPort < 1 || grpcPort > 65535) throw new IllegalArgumentException("Invalid gRPC port");
        activeWorkers.compute(id, (key, address) -> {
            if (address == null) throw new IllegalArgumentException("Worker is not registered");
            if (!address.getHost().equals(host) || address.getContactPort() != contactPort)
                throw new IllegalArgumentException("Endpoint does not match registered worker");
            return new WorkerAddress(host, contactPort, grpcPort);
        });
        // Publication doubles as the lease heartbeat; a worker with no jobs still refreshes here.
        lastSeen.put(id, System.currentTimeMillis());
    }

    /**
     * Removes workers whose last registration/publication is older than {@code ttlMillis}.
     *
     * @return the IDs that were evicted, so the bootstrap can report the membership change
     */
    synchronized List<Integer> expireIdle(long ttlMillis) {
        if (ttlMillis < 1) throw new IllegalArgumentException("A positive TTL is required");
        long cutoff = System.currentTimeMillis() - ttlMillis;
        List<Integer> expired = new ArrayList<>();
        for (Map.Entry<Integer, Long> entry : lastSeen.entrySet()) {
            if (entry.getValue() < cutoff && activeWorkers.remove(entry.getKey()) != null) {
                lastSeen.remove(entry.getKey());
                expired.add(entry.getKey());
            }
        }
        return expired;
    }

    private void persistCounters() {
        if (stateStore != null) {
            stateStore.save(nextWorkerId.get(), nextJobId.get());
        }
    }
}
