package com.cs324.bootstrap;

import com.cs324.common.*;
import com.cs324.computation.GrpcWorkerClient;
import com.cs324.registry.proto.WorkerEndpoint;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** In-memory discovery directory. All network access is through RegistryService. */
public class BootstrapRegistryImpl implements ClusterDirectory {
    private final Map<Integer, WorkerAddress> activeWorkers = new ConcurrentHashMap<>();
    private final AtomicLong nextJobId = new AtomicLong(1);
    private final AtomicInteger nextWorkerId = new AtomicInteger(1);

    @Override public int allocateJobId() {
        long id = nextJobId.getAndIncrement();
        if (id > Integer.MAX_VALUE) throw new IllegalStateException("Job IDs exhausted.");
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
        int id = auto ? nextWorkerId.getAndIncrement() : requestedId;
        List<Map.Entry<Integer, WorkerAddress>> peers = new ArrayList<>(activeWorkers.entrySet());
        Collections.shuffle(peers);
        if (activeWorkers.putIfAbsent(id, new WorkerAddress(host, port)) != null)
            throw new DuplicateWorkerIdException(id);
        nextWorkerId.updateAndGet(current -> Math.max(current, id + 1));
        return peers.stream().filter(e -> e.getValue().getGrpcPort() > 0).findFirst()
                .map(e -> new RegistrationRecord(id, e.getKey(), e.getValue()))
                .orElse(new RegistrationRecord(id, null, null));
    }

    @Override public void deregisterWorker(int id) { activeWorkers.remove(id); }
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
    }
}
