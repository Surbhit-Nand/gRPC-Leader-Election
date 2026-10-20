package com.cs324.computation;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;

// Reuse one plaintext gRPC channel per host and port across concurrent calls.
public final class GrpcChannels {

    private static final ConcurrentHashMap<String, ManagedChannel> CHANNELS = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(GrpcChannels::shutdownAll,
                "grpc-channel-shutdown"));
    }

    private GrpcChannels() {
    }

    public static ManagedChannel get(String host, int port) {
        if (host == null || host.isBlank() || port < 1 || port > 65535) {
            throw new IllegalArgumentException("A valid host and gRPC port are required");
        }
        String key = host + ":" + port;
        return CHANNELS.computeIfAbsent(key, ignored -> ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build());
    }

    // Number of cached channels; used by tests and diagnostics.
    public static int size() {
        return CHANNELS.size();
    }

    public static void shutdownAll() {
        CHANNELS.values().forEach(ManagedChannel::shutdownNow);
        CHANNELS.clear();
    }
}
