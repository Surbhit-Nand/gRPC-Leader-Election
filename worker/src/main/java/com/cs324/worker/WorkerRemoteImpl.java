package com.cs324.worker;

/**
 * Compatibility alias for the former worker implementation name.
 *
 * @deprecated use {@link WorkerRuntime}; this class is local Java only and has
 *             never required RMI since the gRPC migration.
 */
@Deprecated
public class WorkerRemoteImpl extends WorkerRuntime {
    public WorkerRemoteImpl(int id) {
        super(id);
    }
}
