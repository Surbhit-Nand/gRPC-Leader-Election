package com.cs324.bootstrap;

import com.cs324.common.Constants;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
// Entry point for the Bootstrap Node: its own process, holding only the worker registry.
public class BootstrapNode {

    public static void main(String[] args) throws Exception {
        try {
            // Port comes from args, so several clusters can run side by side.
            int port = args.length > 0 ? Integer.parseInt(args[0]) : Constants.DEFAULT_RMI_PORT;
            // createRegistry: this process owns the port, it does not attach to one.
            Registry registry = LocateRegistry.createRegistry(port);
            BootstrapRegistryImpl bootstrapRegistryImpl = new BootstrapRegistryImpl();
            // The name every worker and client looks this stub up under.
            registry.rebind("BootstrapRegistry", bootstrapRegistryImpl);
            System.out.println("Bootstrap Node running on port " + port + " as BootstrapRegistry");

        } catch (Exception e) {
            System.out.println("Failed to start Bootstrap Node: " + e.getMessage());
            System.exit(1);
        }
    }
}
