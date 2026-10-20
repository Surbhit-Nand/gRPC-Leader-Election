package com.cs324.common;

import java.util.List;
import java.util.Set;

// Local control API implemented by the worker and its protobuf/gRPC client.
public interface WorkerControl {
    default int getProgressPort() { return -1; }
    int getId();
    int getJobAllocationCounter();
    boolean isActive();
    int getCurrentCoordinatorId();
    long startElection();
    void ping();
    void connectAsNeighbour(int peerId, String host, int port);
    Set<Integer> getNeighbourIds();
    void notifyLeaving(int leaverId, Set<Integer> neighbours);
    List<WorkerEvent> getRecentEvents(long afterSeq);
}
