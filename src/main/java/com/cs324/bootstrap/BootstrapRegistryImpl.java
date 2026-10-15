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
        WorkerRemote peerStub = findLivePeer();
        // putIfAbsent makes duplicate-id detection atomic; containsKey+put lost updates.
        WorkerAddress prev = activeWorkers.putIfAbsent(workerId, new WorkerAddress(rmiHost, rmiPort));
        // Ids must be unique, so a collision is an error the caller must fix.
        if (prev != null) {
            throw new RemoteException("Duplicate worker id " + workerId + " - already registered");
        }
        // Keep the auto counter past any manual id so a future auto-assign cannot collide.
        nextWorkerId.updateAndGet(cur -> Math.max(cur, workerId + 1));
        return peerStub;
    }

    @Override
    public synchronized WorkerRegistration registerWorkerAuto(String rmiHost, int rmiPort)
            throws RemoteException {
        // Peer first so the newcomer is never handed itself; allocation + insert are one
        // atomic step, so a crash cannot burn an id without registering it.
        WorkerRemote peerStub = findLivePeer();
        int workerId = nextWorkerId.getAndIncrement();
        activeWorkers.put(workerId, new WorkerAddress(rmiHost, rmiPort));
        return new WorkerRegistration(workerId, peerStub);
    }

    @Override
    public List<Integer> getAllocatedWorkerIds() throws RemoteException {
        return getActiveWorkerIds();
    }

    // Shared peer-pick for manual and auto registration: shuffled snapshot, first live wins.
    private WorkerRemote findLivePeer() throws RemoteException {
        // Copy workers before picking a peer, so the newcomer is never handed itself.
        List<Map.Entry<Integer, WorkerAddress>> snapshot = new ArrayList<>(activeWorkers.entrySet());
        // Shuffled per call, so joins are not biased towards the lowest worker id.
        Collections.shuffle(snapshot, new Random());

        // The peer handed back; null means this worker is first in the cluster.
        WorkerRemote peerStub = null;
        // Try candidates in turn, so one dead entry cannot block a join.
        for (Map.Entry<Integer, WorkerAddress> candidate : snapshot) {
            try {
                // Contact the candidate's registry.
                Registry peerRegistry = LocateRegistry.getRegistry(
                        candidate.getValue().getRmiHost(), candidate.getValue().getRmiPort());
                // Look up the peer worker.
                peerStub = (WorkerRemote) peerRegistry.lookup("Worker" + candidate.getKey());
                break; // first live peer wins
            } catch (NotBoundException e) {
                // Stale entry: prune it so it stops blocking future joins.
                // Conditional remove, so a live re-registration is not deleted.
                activeWorkers.remove(candidate.getKey(), candidate.getValue());
            } catch (RemoteException e) {
                // Peer unreachable right now: try the next candidate.
            }
        }
        return peerStub;
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
