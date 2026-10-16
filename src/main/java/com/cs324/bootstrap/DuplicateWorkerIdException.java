package com.cs324.bootstrap;

import java.rmi.RemoteException;

// Claimed worker id already taken. A subclass of RemoteException, so existing
// RMI callers catching RemoteException (and message assertions on
// "Duplicate worker id") keep working; the gRPC service maps it to
// ALREADY_EXISTS instead of a generic transport failure.
public class DuplicateWorkerIdException extends RemoteException {

    public DuplicateWorkerIdException(int workerId) {
        super("Duplicate worker id " + workerId + " - already registered");
    }
}
