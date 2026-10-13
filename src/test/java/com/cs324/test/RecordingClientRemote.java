package com.cs324.test;

import com.cs324.common.ClientRemote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

// Test double for ClientRemote that keeps every result the coordinator delivers.
// callOrder is separate from the map, so a re-delivery stays visible as a second call.
public class RecordingClientRemote extends UnicastRemoteObject implements ClientRemote {

    // Latest value per job, so a test reads the aggregate the coordinator actually sent.
    private final Map<Integer, Object> results = new ConcurrentHashMap<>();
    // Arrival order, so callCount counts deliveries rather than distinct jobs.
    private final ConcurrentLinkedQueue<Integer> callOrder = new ConcurrentLinkedQueue<>();

    public RecordingClientRemote() throws RemoteException {
        super();
    }

    @Override
    public void receiveJobResult(int jobId, Object result) throws RemoteException {
        results.put(jobId, result);
        callOrder.add(jobId);
    }

    // Null means the job never arrived, which is how a test asserts nothing was delivered.
    public Object resultFor(int jobId) {
        return results.get(jobId);
    }

    public boolean received(int jobId) {
        return results.containsKey(jobId);
    }

    // Counts deliveries, not jobs, so a double delivery is detectable.
    public int callCount() {
        return callOrder.size();
    }
}