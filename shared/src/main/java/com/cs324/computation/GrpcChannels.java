package com.cs324.computation;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared cache of plaintext gRPC channels keyed by {@code host:port}.
 *
 * <p>Control probes, assignments, result reports and monitoring all talk to the
 * same small set of peers. Creating and shutting down a channel for every RPC
 * works for a demo but pays a TCP/TLS/HTTP2 setup cost on every probe and can
 * exhaust transient ports under load. A channel is safe for concurrent RPCs and
 * reconnects internally, so one channel per peer is sufficient.
 */
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

    /** Number of cached channels; used by tests and diagnostics. */
    public static int size() {
        return CHANNELS.size();
    }

    public static void shutdownAll() {
        CHANNELS.values().forEach(ManagedChannel::shutdownNow);
        CHANNELS.clear();
    }
}
