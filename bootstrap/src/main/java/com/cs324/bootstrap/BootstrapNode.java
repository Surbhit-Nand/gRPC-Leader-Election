package com.cs324.bootstrap;

import com.cs324.common.Constants;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Entry point for the Bootstrap Node: its own process, holding only the worker registry. The executable serves gRPC only.
public class BootstrapNode {

    public static void main(String[] args) throws Exception {
        try {
            // Usage: BootstrapNode [grpcPort] [--state-file path]. Ports come from args, so several clusters can run side by side.
            String stateFile = System.getProperty("bootstrap.state.file", "bootstrap-state.properties");
            List<String> positional = new ArrayList<>();
            for (int i = 0; i < args.length; i++) {
                if ("--state-file".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--state-file needs a path.");
                    }
                    stateFile = args[++i];
                } else if (args[i].startsWith("--")) {
                    throw new IllegalArgumentException("Unknown option: " + args[i]);
                } else {
                    positional.add(args[i]);
                }
            }
            if (positional.size() > 1) {
                throw new IllegalArgumentException("Expected at most one positional argument: [grpcPort].");
            }
            int grpcPort = positional.isEmpty()
                    ? Constants.DEFAULT_GRPC_PORT
                    : Integer.parseInt(positional.get(0));

            BootstrapStateStore stateStore = new BootstrapStateStore(Path.of(stateFile));
            BootstrapRegistryImpl bootstrapRegistryImpl = new BootstrapRegistryImpl(stateStore);

            Server grpcServer = ServerBuilder.forPort(grpcPort)
                    .addService(new BootstrapGrpcService(bootstrapRegistryImpl))
                    .build()
                    .start();

            // Expire stale directory entries so crashed workers do not remain in quorum counts.
            ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "bootstrap-lease-reaper");
                thread.setDaemon(true);
                return thread;
            });
            reaper.scheduleWithFixedDelay(() -> {
                try {
                    List<Integer> expired = bootstrapRegistryImpl.expireIdle(Constants.BOOTSTRAP_WORKER_TTL_MS);
                    if (!expired.isEmpty()) {
                        System.out.println("Expired inactive worker lease(s): " + expired);
                    }
                } catch (RuntimeException e) {
                    System.err.println("Worker lease reaper failed: " + e.getMessage());
                }
            }, Constants.BOOTSTRAP_LEASE_INTERVAL_MS, Constants.BOOTSTRAP_LEASE_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);

            System.out.println("Bootstrap gRPC RegistryService on port " + grpcServer.getPort());
            System.out.println("Bootstrap ID state: " + stateStore.file());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                reaper.shutdownNow();
                grpcServer.shutdownNow();
            }));
            grpcServer.awaitTermination();

        } catch (Exception e) {
            System.out.println("Failed to start Bootstrap Node: " + e.getMessage());
            System.exit(1);
        }
    }
}
