package com.cs324.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

// RMI contract every Worker node implements: discovery, election, and job execution.
public interface WorkerRemote extends Remote {

    // Identity, and the tie-break used when two workers share a JAC.
    int getId() throws RemoteException;

    // Work shed to other workers; the metric candidates are compared on.
    int getJobAllocationCounter() throws RemoteException;

    // Liveness probe target: a JVM that stops answering counts as failed.
    boolean isActive() throws RemoteException;

    // Current known coordinator id (-1 if none has been elected yet).
    int getCurrentCoordinatorId() throws RemoteException;

    // Starts a new election wave; returns the unique message id used.
    long startElection() throws RemoteException;

    // Heartbeat probe: succeeds as long as this worker's JVM is responsive.
    void ping() throws RemoteException;

    // Client entry point: the coordinator splits the payload, then calls the client back.
    void submitJob(int jobId, String jobType, Object payload,
                   String clientHost, int clientPort, String clientName) throws RemoteException;

    // Neighbour-to-neighbour election wave; originId and messageId expose duplicates.
    void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException;

    // Neighbour-to-neighbour reply, sent once a subtree has finished the wave.
    void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException;

    // Adds a peer; both sides call it, so the neighbour edge ends up bidirectional.
    void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException;

    // The winning candidate's verdict, relayed outward by every node that hears it.
    void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException;

    // Coordinator hands out one slice; the worker replies through reportTaskResult.
    void assignTask(int jobId, String jobType, Object payload) throws RemoteException;   // fire-and-return
    void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException; // callback when done
}
