package com.cs324.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

// RMI contract for the Bootstrap Node: an active-worker directory, never a participant.
public interface BootstrapRemote extends Remote {

    // A worker registers and is returned one active peer to join, or null if it is first.
    WorkerRemote registerWorker(int workerId, String rmiHost, int rmiPort) throws RemoteException;

    // Fired on clean shutdown, so the directory stops handing out dead workers.
    void deregisterWorker(int workerId) throws RemoteException;

    // Shared IDs keep jobs from different clients separate. No computation happens here.
    int allocateJobId() throws RemoteException;

    // Lets a coordinator reach every worker, not only its own neighbours.
    List<Integer> getActiveWorkerIds() throws RemoteException;

    // Address lookup so a joiner can reach the peer stub it was handed; null if unknown.
    com.cs324.common.WorkerAddress getWorkerAddress(int workerId) throws RemoteException;
}
