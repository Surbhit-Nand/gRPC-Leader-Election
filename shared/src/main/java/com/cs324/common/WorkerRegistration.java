package com.cs324.common;

// Atomically allocated worker identity and one peer to join, if available.
public record WorkerRegistration(int workerId, WorkerControl peerStub) {
}
