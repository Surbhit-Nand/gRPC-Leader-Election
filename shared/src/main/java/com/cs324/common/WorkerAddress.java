package com.cs324.common;

// Directory data. Network calls use host + grpcPort; contactPort is legacy metadata
// retained only for wire compatibility and is never dialled.
public final class WorkerAddress {
    private final String host;
    private final int contactPort;
    private final int grpcPort;

    public WorkerAddress(String host, int contactPort) {
        this(host, contactPort, 0);
    }

    public WorkerAddress(String host, int contactPort, int grpcPort) {
        this.host = host;
        this.contactPort = contactPort;
        this.grpcPort = grpcPort;
    }

    public String getHost() {
        return host;
    }

    public int getContactPort() {
        return contactPort;
    }

    public int getGrpcPort() {
        return grpcPort;
    }
}
