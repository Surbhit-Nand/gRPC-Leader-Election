package com.cs324.test;

import com.cs324.common.WorkerRemote;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

// Silent test double for WorkerRemote: only getId() carries meaning, the rest are no-ops.
// Implemented in full so it can be bound in a real RMI registry and looked up by name.
public class FakeWorkerRemote extends UnicastRemoteObject implements WorkerRemote {

    private final int id;

    public FakeWorkerRemote(int id) throws RemoteException {
        super();
        this.id = id;
    }

    @Override
    public int getId() throws RemoteException {
        return id;
    }

    @Override
    public int getJobAllocationCounter() throws RemoteException {
        return 0;
    }

    @Override
    public int getCurrentCoordinatorId() throws RemoteException {
        return id;
    }

    @Override
    public long startElection() throws RemoteException {
        return 0L;
    }

    @Override
    public void ping() throws RemoteException {
        // not needed for these tests
    }

    @Override
    public void submitJob(int jobId, String jobType, Object payload, String clientHost, int clientPort, String clientName) throws RemoteException {
        // not needed for these tests
    }

    @Override
    public boolean isActive() throws RemoteException {
        return true;
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException {
        // not needed for these tests
    }

    @Override
    public void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException {
        // not needed for these tests
    }

    @Override
    public void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException {
        // not needed for these tests
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        // not needed for these tests
    }

    @Override
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        // not needed for these tests
    }

    @Override
    public void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException {
        // not needed for these tests
    }
}