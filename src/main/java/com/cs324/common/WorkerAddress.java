package com.cs324.common;

// Host and RMI port of one worker, as published by the bootstrap directory.
public class WorkerAddress implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    // Package-private: only the registry populates these and RMI serialises them.
    String rmiHost;
    int rmiPort;

        // Called by the registry when it hands out a worker's address.
    public WorkerAddress(String Host, int Port){
        rmiHost = Host;
        rmiPort = Port;
    }

    public String getRmiHost(){
        return rmiHost;
    }

    public int getRmiPort(){
        return rmiPort;
    }

}
