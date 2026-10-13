package com.cs324.client;

import com.cs324.common.ClientRemote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

// Lives in the Client process and receives the aggregate once every slice has reported.
public class ClientRemoteImpl extends UnicastRemoteObject implements ClientRemote {

    // Indirection so this class needs no knowledge of the GUI layer.
    private final ResultHandler handler;

    // Lets the caller supply its own handler, typically an event-dispatch lambda.
    public interface ResultHandler {
        void handle(int jobId, Object result);
    }

    public ClientRemoteImpl(ResultHandler handler) throws RemoteException {
        this.handler = handler;
    }

    @Override
        // Arrives on an RMI thread, so the handler hops to the EDT itself.
    public void receiveJobResult(int jobId, Object result) throws RemoteException {
        handler.handle(jobId, result);
    }
}