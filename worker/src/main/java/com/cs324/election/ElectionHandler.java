package com.cs324.election;

import com.cs324.election.proto.CoordinatorAnnouncement;
import com.cs324.election.proto.EchoMessage;
import com.cs324.election.proto.ElectionMessage;

/**
 * The election algorithm's entry points, in the shape {@link ElectionGrpcService}
 * needs. Implemented by the worker, so the service holds no election state of its
 * own and the protocol has exactly one home.
 */
public interface ElectionHandler {

    /** Folds an incoming wave in and forwards it onward. */
    void onElectionMessage(ElectionMessage message);

    /** Folds a child's answer in; the wave completes when nobody is outstanding. */
    void onEchoMessage(EchoMessage message);

    /** Applies the winner's verdict and relays it outward. */
    void onCoordinatorAnnouncement(CoordinatorAnnouncement announcement);
}