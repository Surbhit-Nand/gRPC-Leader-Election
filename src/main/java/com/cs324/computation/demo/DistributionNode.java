package com.cs324.computation.demo;

import com.cs324.computation.GrpcJobService;
import com.cs324.computation.GrpcTaskService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Standalone Member 3 harness; manual membership and coordinator, no bootstrap/election. */
public final class DistributionNode {
    private DistributionNode() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || !(args[0].equals("worker") || args[0].equals("coordinator"))) {
            throw new IllegalArgumentException(
                    "Usage: DistributionNode worker|coordinator <workerId> <grpcPort> [id@host:port ...]");
        }
        boolean coordinator = args[0].equals("coordinator");
        int id = Integer.parseInt(args[1]);
        int port = Integer.parseInt(args[2]);
        if (id < 1 || port < 1 || port > 65535)
            throw new IllegalArgumentException("Use a positive worker ID and a port between 1 and 65535");
        if (!coordinator && args.length != 3)
            throw new IllegalArgumentException("Only the coordinator takes peer addresses");
        List<GrpcJobService.Endpoint> peers = new ArrayList<>();
        java.util.Set<Integer> ids = new java.util.HashSet<>();
        ids.add(id);
        for (int i = 3; i < args.length; i++) {
            int at = args[i].indexOf('@');
            int colon = args[i].lastIndexOf(':');
            if (at < 1 || colon <= at + 1)
                throw new IllegalArgumentException("Peer format: id@host:port");
            int peerId = Integer.parseInt(args[i].substring(0, at));
            if (peerId < 1 || !ids.add(peerId))
                throw new IllegalArgumentException("Worker IDs must be positive and unique");
            peers.add(new GrpcJobService.Endpoint(peerId, args[i].substring(at + 1, colon),
                    Integer.parseInt(args[i].substring(colon + 1))));
        }
        List<GrpcJobService.Endpoint> membership = List.copyOf(peers);
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors()));
        AtomicInteger jac = new AtomicInteger();
        ServerBuilder<?> builder = ServerBuilder.forPort(port).addService(new GrpcTaskService(id, pool));
        if (coordinator) {
            builder.addService(new GrpcJobService(id, () -> membership, () -> true, target ->
                    System.out.println("Assigned slice to worker " + target + "; JAC=" + jac.incrementAndGet()),
                    pool, Duration.ofMinutes(10)));
        }
        Server server;
        try { server = builder.build().start(); }
        catch (Exception e) { pool.shutdownNow(); throw e; }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdownNow();
            pool.shutdownNow();
            try { server.awaitTermination(5, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }, "distribution-shutdown"));
        System.out.println("Ready: " + args[0] + " " + id + " on gRPC port " + server.getPort());
        server.awaitTermination();
    }
}
