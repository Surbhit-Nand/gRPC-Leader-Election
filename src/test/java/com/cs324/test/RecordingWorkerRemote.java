package com.cs324.test;

import com.cs324.common.WorkerRemote;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// Test double that records every remote call it receives.
// Unlike FakeWorkerRemote it keeps an audit trail, which is what makes delivery assertable.
// Every method is implemented, not just the ones a test uses, so it can be bound by name.
public class RecordingWorkerRemote extends UnicastRemoteObject implements WorkerRemote {

    private final int id;
    // One append-only log for every call kind, which the helpers below match by prefix.
    private final List<String> calls = new CopyOnWriteArrayList<>();

    public RecordingWorkerRemote(int id) throws RemoteException {
        super();
        this.id = id;
    }


    // A copy, so a test can iterate while RMI threads keep appending.
    public List<String> calls() {
        return List.copyOf(calls);
    }

    public boolean receivedElection(int originId, long messageId) {
        return calls.contains("election|" + originId + "|" + messageId);
    }

    public boolean receivedEcho(int fromWorkerId, long messageId) {
        return calls.contains("echo|" + fromWorkerId + "|" + messageId);
    }

    public boolean receivedCoordinator(int coordinatorId, long messageId) {
        return calls.contains("coordinator|" + coordinatorId + "|" + messageId);
    }

    public boolean receivedAssignment(int jobId, String jobType) {
        return calls.contains("assign|" + jobId + "|" + jobType);
    }

    public boolean receivedResult(int jobId, int workerId) {
        return calls.stream().anyMatch(call -> call.startsWith("result|" + jobId + "|" + workerId + "|"));
    }

    // The newest value wins, because the log is append-only and in arrival order.
    public String lastReportedResult(int jobId, int workerId) {
        String prefix = "result|" + jobId + "|" + workerId + "|";
        String last = null;
        for (String call : calls) {
            if (call.startsWith(prefix)) {
                last = call.substring(prefix.length());
            }
        }
        return last;
    }


    @Override
    public int getId() throws RemoteException {
        return id;
    }

    @Override
    public int getJobAllocationCounter() throws RemoteException {
        // Zero keeps the default election outcome (higher id wins) for tests that do not set it.
        return 0;
    }

    @Override
    public int getCurrentCoordinatorId() throws RemoteException {
        return id;
    }

    @Override
    public long startElection() throws RemoteException {
        // A fixed return id, so tests match on the recorded call rather than the value.
        calls.add("startElection");
        return 0L;
    }

    @Override
    public void ping() throws RemoteException {
        calls.add("ping");
    }

    @Override
    public void submitJob(int jobId, String jobType, Object payload, String clientHost, int clientPort, String clientName) throws RemoteException {
        calls.add("submit|" + jobId + "|" + jobType);
    }

    @Override
    public boolean isActive() throws RemoteException {
        return true;
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException {
        // Only origin and wave id are recorded, which is all the assertions match on.
        calls.add("election|" + originId + "|" + messageId);
    }

    @Override
    public void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException {
        calls.add("echo|" + fromWorkerId + "|" + messageId);
    }

    @Override
    public void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException {
        calls.add("connect|" + newWorker + "|" + rmiHost + "|" + rmiPort);
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        calls.add("coordinator|" + coordinatorId + "|" + messageId);
    }

    @Override
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        calls.add("assign|" + jobId + "|" + jobType);
    }

    @Override
        // The result is stored as text, which is how lastReportedResult reads it back.
    public void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException {
        calls.add("result|" + jobId + "|" + workerId + "|" + result);
    }
}
