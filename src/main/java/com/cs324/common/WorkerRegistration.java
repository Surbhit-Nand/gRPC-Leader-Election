package com.cs324.common;

import java.io.Serializable;

// Result of an automatic worker registration: the allocated id plus one live peer to join.
// Returned by BootstrapRemote.registerWorkerAuto so id allocation and directory insert are atomic.
public record WorkerRegistration(int workerId, WorkerRemote peerStub) implements Serializable {
}
