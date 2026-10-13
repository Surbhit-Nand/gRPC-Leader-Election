package com.cs324.test;

import com.cs324.common.WorkerRemote;

import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicInteger;

// Recording worker that finishes an election wave and relays coordinator announcements.
// The parent only records a wave, so a wave sent to it alone never concludes.
// Echoing the best candidate back to the origin is what lets the wave finish.
// Relaying announcements back to the sender too makes a two-node ring cyclic and loop-back visible.
public class EchoingWorkerRemote extends RecordingWorkerRemote {

    private final AtomicInteger jac = new AtomicInteger(0);
    private volatile WorkerRemote relayTarget;

    public EchoingWorkerRemote(int id) throws RemoteException {
        super(id);
    }

    // Settable so a test can pick the wave's winner instead of inheriting the higher-id default.
    public void setJobAllocationCounter(int jobs) {
        this.jac.set(jobs);
    }

    // Must name the origin, or the wave has no one to answer and never completes.
    public void relayAnnouncementsTo(WorkerRemote neighbour) {
        this.relayTarget = neighbour;
    }

    @Override
    public int getJobAllocationCounter() throws RemoteException {
        return jac.get();
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId)
            throws RemoteException {
        super.receiveElectionMessage(originId, candidateId, candidateJac, messageId);
        WorkerRemote origin = relayTarget;
        if (origin == null) {
            return;
        }
        // A childless node answers immediately, reporting whichever of the two JACs is better.
        boolean weWin = jac.get() < candidateJac
                || (jac.get() == candidateJac && getId() > candidateId);
        origin.receiveEchoMessage(messageId, getId(),
                weWin ? getId() : candidateId, weWin ? jac.get() : candidateJac);
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        super.receiveCoordinatorMessage(coordinatorId, messageId);
        // Flooding every direction is what lets an announcement circle back to its sender.
        WorkerRemote target = relayTarget;
        if (target != null) {
            target.receiveCoordinatorMessage(coordinatorId, messageId);
        }
    }
}
