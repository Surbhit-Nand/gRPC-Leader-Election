package com.cs324.common;

import java.util.List;

// Local directory API; RegistryClient implements it using protobuf/gRPC.
public interface ClusterDirectory {

    // A worker registers and is returned one active peer to join, or null if it is first.
    WorkerControl registerWorker(int workerId, String host, int port);

    // Fired on clean shutdown, so the directory stops handing out dead workers.
    void deregisterWorker(int workerId);

    // Shared IDs keep jobs from different clients separate. No computation happens here.
    int allocateJobId();

    // Lets a coordinator reach every worker, not only its own neighbours.
    List<Integer> getActiveWorkerIds();

    // Allocate an ID and register its address atomically, returning one peer to join.
    WorkerRegistration registerWorkerAuto(String host, int port);

    // All ids ever handed out that are still registered (debug/observability).
    List<Integer> getAllocatedWorkerIds();

    // Address lookup so a joiner can reach the peer stub it was handed; null if unknown.
    WorkerAddress getWorkerAddress(int workerId);
}
