package com.cs324.election;

import com.cs324.common.WorkerRemote;

import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * Legacy RMI hop, kept so a cluster without published gRPC endpoints - and every
 * existing election test - keeps its exact previous behaviour.
 *
 * <p>The neighbour graph itself is unchanged: an RMI stub is looked up in the same
 * {@code neighbours} map that holds the topology, so membership, repair and the
 * 2-hop view stay where they are and only the wire format differs.
 */
public final class RmiElectionTransport implements ElectionTransport {

    private final IntFunction<WorkerRemote> stubs;
    private final Consumer<String> log;

    public RmiElectionTransport(IntFunction<WorkerRemote> stubs, Consumer<String> log) {
        this.stubs = stubs;
        this.log = log;
    }

    @Override
    public boolean sendElection(int peerId, int originId, int candidateId, int candidateJac, long messageId, long term) {
        try {
            WorkerRemote peer = stubs.apply(peerId);
            if (peer == null) {
                log.accept("no RMI stub for Worker " + peerId);
                return false;
            }
            peer.receiveElectionMessage(originId, candidateId, candidateJac, messageId);
            return true;
        } catch (Exception e) {
            log.accept("RMI election hop to Worker " + peerId + " failed: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean sendEcho(int peerId, long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac, long term) {
        try {
            WorkerRemote peer = stubs.apply(peerId);
            if (peer == null) {
                log.accept("no RMI stub for Worker " + peerId);
                return false;
            }
            peer.receiveEchoMessage(messageId, fromWorkerId, bestCandidateId, bestCandidateJac);
            return true;
        } catch (Exception e) {
            log.accept("RMI echo hop to Worker " + peerId + " failed: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean sendAnnouncement(int peerId, int coordinatorId, long messageId, long term) {
        try {
            WorkerRemote peer = stubs.apply(peerId);
            if (peer == null) {
                log.accept("no RMI stub for Worker " + peerId);
                return false;
            }
            peer.receiveCoordinatorMessage(coordinatorId, messageId);
            return true;
        } catch (Exception e) {
            log.accept("RMI announcement hop to Worker " + peerId + " failed: " + e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        // Nothing of our own to release: the stubs belong to the neighbour map.
    }
}