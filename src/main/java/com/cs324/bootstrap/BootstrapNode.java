package com.cs324.bootstrap;

import com.cs324.common.Constants;
import io.grpc.Server;
import io.grpc.ServerBuilder;
// Entry point for the Bootstrap Node: its own process, holding only the worker registry.
// The executable serves gRPC only; old argument positions remain compatible.
public class BootstrapNode {

    public static void main(String[] args) throws Exception {
        try {
            // Ports come from args, so several clusters can run side by side.
            // Usage: BootstrapNode [legacyUnusedPort] [grpcPort].
            int port = args.length > 0 ? Integer.parseInt(args[0]) : Constants.DEFAULT_RMI_PORT;
            int grpcPort = args.length > 1 ? Integer.parseInt(args[1]) : Constants.DEFAULT_GRPC_PORT;
            BootstrapRegistryImpl bootstrapRegistryImpl = new BootstrapRegistryImpl(false);

            // Explicitly await the listener: no RMI threads keep this process alive.
            Server grpcServer = ServerBuilder.forPort(grpcPort)
                    .addService(new BootstrapGrpcService(bootstrapRegistryImpl))
                    .build()
                    .start();
            System.out.println("Bootstrap gRPC RegistryService on port " + grpcServer.getPort());
            Runtime.getRuntime().addShutdownHook(new Thread(grpcServer::shutdownNow));
            grpcServer.awaitTermination();

        } catch (Exception e) {
            System.out.println("Failed to start Bootstrap Node: " + e.getMessage());
            System.exit(1);
        }
    }
}
