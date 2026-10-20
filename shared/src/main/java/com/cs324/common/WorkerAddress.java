package com.cs324.common;

// Network calls use host and grpcPort; contactPort is unused compatibility metadata.
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
