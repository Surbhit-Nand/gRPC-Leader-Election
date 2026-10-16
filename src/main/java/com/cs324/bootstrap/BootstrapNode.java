package com.cs324.bootstrap;

import com.cs324.common.Constants;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
// Entry point for the Bootstrap Node: its own process, holding only the worker registry.
// Serves the directory over RMI and gRPC at once (dual-stack migration): election and
// job traffic still use RMI, but registration already speaks both transports.
public class BootstrapNode {

    public static void main(String[] args) throws Exception {
        try {
            // Ports come from args, so several clusters can run side by side.
            // Usage: BootstrapNode [rmiPort] [grpcPort].
            int port = args.length > 0 ? Integer.parseInt(args[0]) : Constants.DEFAULT_RMI_PORT;
            int grpcPort = args.length > 1 ? Integer.parseInt(args[1]) : Constants.DEFAULT_GRPC_PORT;
            BootstrapRegistryImpl bootstrapRegistryImpl = new BootstrapRegistryImpl();
            // createRegistry: this process owns the port, it does not attach to one.
            Registry registry = LocateRegistry.createRegistry(port);
            // The name every worker and client looks this stub up under.
            registry.rebind("BootstrapRegistry", bootstrapRegistryImpl);
            System.out.println("Bootstrap Node running on port " + port + " as BootstrapRegistry");

            // Same directory, second transport. RMI threads keep the JVM alive,
            // so the server only needs starting, never blocking on.
            Server grpcServer = ServerBuilder.forPort(grpcPort)
                    .addService(new BootstrapGrpcService(bootstrapRegistryImpl))
                    .build()
                    .start();
            System.out.println("Bootstrap gRPC RegistryService on port " + grpcServer.getPort());

        } catch (Exception e) {
            System.out.println("Failed to start Bootstrap Node: " + e.getMessage());
            System.exit(1);
        }
    }
}
