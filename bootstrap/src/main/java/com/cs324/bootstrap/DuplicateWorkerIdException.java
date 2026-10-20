package com.cs324.bootstrap;


// Claimed worker id already taken; the gRPC service maps it to ALREADY_EXISTS.
public class DuplicateWorkerIdException extends IllegalStateException {

    public DuplicateWorkerIdException(int workerId) {
        super("Duplicate worker id " + workerId + " - already registered");
    }
}
