package com.cs324.common;

// Result of an automatic worker registration: the allocated id plus one live peer to join.
// Returned by ClusterDirectory.registerWorkerAuto so id allocation and directory insert are atomic.
public record WorkerRegistration(int workerId, WorkerControl peerStub) {
}
