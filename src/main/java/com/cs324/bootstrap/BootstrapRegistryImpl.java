package com.cs324.bootstrap;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerRegistration;
import com.cs324.common.WorkerRemote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Active-worker directory plus a job-id counter; it never takes part in elections.
public class BootstrapRegistryImpl extends UnicastRemoteObject implements BootstrapRemote {

    // Concurrent: workers register and deregister in parallel.
    private final Map<Integer, WorkerAddress> activeWorkers = new ConcurrentHashMap<>();

    // Cluster-wide job ids, so two client windows cannot collide on one id.
    private final AtomicLong nextJobId = new AtomicLong(1);

    // Monotonic worker ids for auto-registration; never reused (deregister does not rewind it).
    // In-memory only, so a Bootstrap restart resets the sequence to 1 alongside the directory.
    private final AtomicInteger nextWorkerId = new AtomicInteger(1);

    @Override
    public int allocateJobId() throws RemoteException {
        // Allocate once for the whole cluster, rather than once per client window.
        long jobId = nextJobId.getAndIncrement();
        // The RMI contract exposes int, so refuse to wrap silently.
        if (jobId > Integer.MAX_VALUE) throw new RemoteException("Job IDs exhausted.");
        return (int) jobId;
    }

    public BootstrapRegistryImpl() throws RemoteException {
        super();
    }

    @Override
    public WorkerRemote registerWorker(int workerId, String rmiHost, int rmiPort) throws RemoteException {
        RegistrationRecord record = registerRecord(workerId, rmiHost, rmiPort);
        return stubForPeer(record);
    }

    @Override
    public WorkerRegistration registerWorkerAuto(String rmiHost, int rmiPort)
            throws RemoteException {
        RegistrationRecord record = registerAutoRecord(rmiHost, rmiPort);
        return new WorkerRegistration(record.workerId(), stubForPeer(record));
    }

    // Registration result as data, without RMI stubs: the backing call for the
    // gRPC service. peerId/peerAddress are null when this worker is first.
    record RegistrationRecord(int workerId, Integer peerId, WorkerAddress peerAddress) {}

    @Override
    public List<Integer> getAllocatedWorkerIds() throws RemoteException {
        return getActiveWorkerIds();
    }

    // Manual claim of one id; duplicates fail with DuplicateWorkerIdException.
    synchronized RegistrationRecord registerRecord(int workerId, String rmiHost, int rmiPort)
            throws DuplicateWorkerIdException {
        // Peer first so the newcomer is never handed itself.
        PeerSelection peer = findLivePeer();
        // putIfAbsent makes duplicate-id detection atomic; containsKey+put lost updates.
        // Ids must be unique, so a collision is an error the caller must fix.
        WorkerAddress prev = activeWorkers.putIfAbsent(workerId, new WorkerAddress(rmiHost, rmiPort));
        if (prev != null) {
            throw new DuplicateWorkerIdException(workerId);
        }
        // Keep the auto counter past any manual id so a future auto-assign cannot collide.
        nextWorkerId.updateAndGet(cur -> Math.max(cur, workerId + 1));
        return toRecord(workerId, peer);
    }

    // Monotonic allocation plus directory insert in one atomic step, so a crash
    // cannot burn an id without registering it.
    synchronized RegistrationRecord registerAutoRecord(String rmiHost, int rmiPort) {
        // Peer first so the newcomer is never handed itself.
        PeerSelection peer = findLivePeer();
        int workerId = nextWorkerId.getAndIncrement();
        activeWorkers.put(workerId, new WorkerAddress(rmiHost, rmiPort));
        return toRecord(workerId, peer);
    }

    // Null peer means this worker is first: no id or address to hand back.
    private static RegistrationRecord toRecord(int workerId, PeerSelection peer) {
        return peer == null
                ? new RegistrationRecord(workerId, null, null)
                : new RegistrationRecord(workerId, peer.peerId(), peer.address());
    }

    // Resolves the recorded peer back to a live stub; null when first, or when
    // the peer died in the race between the pick and this lookup.
    private static WorkerRemote stubForPeer(RegistrationRecord record) {
        if (record.peerId() == null) {
            return null;
        }
        try {
            Registry peerRegistry = LocateRegistry.getRegistry(
                    record.peerAddress().getRmiHost(), record.peerAddress().getRmiPort());
            return (WorkerRemote) peerRegistry.lookup("Worker" + record.peerId());
        } catch (Exception e) {
            return null;
        }
    }

    // Chosen join peer: id plus published address plus the stub that proved it
    // live, all from one lookup. Null means this worker is first in the cluster.
    private record PeerSelection(int peerId, WorkerAddress address, WorkerRemote stub) {}

    // Shared peer-pick for manual and auto registration: shuffled snapshot, first live wins.
    private PeerSelection findLivePeer() {
        // Copy workers before picking a peer, so the newcomer is never handed itself.
        List<Map.Entry<Integer, WorkerAddress>> snapshot = new ArrayList<>(activeWorkers.entrySet());
        // Shuffled per call, so joins are not biased towards the lowest worker id.
        Collections.shuffle(snapshot, new Random());

        // Try candidates in turn, so one dead entry cannot block a join.
        for (Map.Entry<Integer, WorkerAddress> candidate : snapshot) {
            try {
                // Contact the candidate's registry.
                Registry peerRegistry = LocateRegistry.getRegistry(
                        candidate.getValue().getRmiHost(), candidate.getValue().getRmiPort());
                // Look up the peer worker; the first live peer wins.
                WorkerRemote peerStub =
                        (WorkerRemote) peerRegistry.lookup("Worker" + candidate.getKey());
                return new PeerSelection(candidate.getKey(), candidate.getValue(), peerStub);
            } catch (NotBoundException e) {
                // Stale entry: prune it so it stops blocking future joins.
                // Conditional remove, so a live re-registration is not deleted.
                activeWorkers.remove(candidate.getKey(), candidate.getValue());
            } catch (RemoteException e) {
                // Peer unreachable right now: try the next candidate.
            }
        }
        // No live peer: this worker is first, so there is nothing to join.
        return null;
    }

    @Override
    public void deregisterWorker(int workerId) throws RemoteException {
        activeWorkers.remove(workerId);
    }

    @Override
    public List<Integer> getActiveWorkerIds() throws RemoteException {
        // A snapshot, so the caller can iterate while workers come and go.
        return new ArrayList<>(activeWorkers.keySet());
    }

    @Override
    public WorkerAddress getWorkerAddress(int workerId) throws RemoteException {
        // Null when unknown, which is how callers detect a stale peer.
        return activeWorkers.get(workerId);
    }
}
