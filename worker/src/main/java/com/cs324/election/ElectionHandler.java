package com.cs324.election;

import com.cs324.election.proto.CoordinatorAnnouncement;
import com.cs324.election.proto.EchoMessage;
import com.cs324.election.proto.ElectionMessage;

// Worker-owned election state is accessed through these RPC handlers.
public interface ElectionHandler {

    // Folds an incoming wave in and forwards it onward.
    void onElectionMessage(ElectionMessage message);

    // Folds a child's answer in; the wave completes when nobody is outstanding.
    void onEchoMessage(EchoMessage message);

    // Applies the winner's verdict and relays it outward.
    void onCoordinatorAnnouncement(CoordinatorAnnouncement announcement);
}