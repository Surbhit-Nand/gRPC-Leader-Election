package com.cs324.common;

import java.io.Serializable;
import java.util.Objects;

// Carries one chunk plus the coordinator that must receive its answer.
public record TaskAssignment(Object payload, WorkerRemote coordinator) implements Serializable {
    public TaskAssignment {
        // Fail fast: a chunk with no coordinator could never report back.
        Objects.requireNonNull(coordinator, "coordinator");
    }
}
