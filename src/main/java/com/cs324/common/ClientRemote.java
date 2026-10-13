package com.cs324.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

// RMI contract a Client node implements; the coordinator calls back with a job result.
public interface ClientRemote extends Remote {

    // Delivers one aggregated result; the client matches it back to the job by id.
    void receiveJobResult(int jobId, Object result) throws RemoteException;
}