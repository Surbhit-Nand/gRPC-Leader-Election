package com.cs324.failover;

import com.cs324.bootstrap.*;
import com.cs324.computation.RegistryClient;
import com.cs324.failover.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationAdmissionTest {
    private boolean replicate(long ackSequence, boolean fail) throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        Server peer = ServerBuilder.forPort(0).addService(new FailoverServiceGrpc.FailoverServiceImplBase() {
            @Override public void fetchCheckpoints(FetchRequest request, StreamObserver<FetchReply> response) {
                response.onNext(FetchReply.getDefaultInstance());
                response.onCompleted();
            }
            @Override public void replicate(ReplicateRequest request, StreamObserver<ReplicateAck> response) {
                if (fail) { response.onError(Status.UNAVAILABLE.asRuntimeException()); return; }
                response.onNext(ReplicateAck.newBuilder().setAcknowledgedSequence(ackSequence).build());
                response.onCompleted();
            }
        }).build().start();
        Server bootstrap = ServerBuilder.forPort(0).addService(new BootstrapGrpcService(new BootstrapRegistryImpl())).build().start();
        try (var directory = new RegistryClient("localhost", bootstrap.getPort());
             var replicator = new CheckpointReplicator(1, directory, pool, ignored -> {})) {
            directory.registerWorker(2, "localhost", 0);
            directory.publish(2, "localhost", 0, peer.getPort());
            boolean confirmed = replicator.replicate(List.of(CheckpointEntry.newBuilder().setJobId(1).setSequence(8).setTerm(1)
                    .setAccepted(JobAccepted.getDefaultInstance()).build()), 1, true);
            replicator.close();
            assertNull(replicator.fetch(directory.worker(2)), "closed replication must not reopen channels");
            return confirmed;
        } finally {
            peer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            bootstrap.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    @Test void failedReplicaDoesNotConfirmAcceptance() throws Exception { assertFalse(replicate(8, true)); }
    @Test void staleAcknowledgementDoesNotConfirmAcceptance() throws Exception { assertFalse(replicate(7, false)); }
    @Test void matchingAcknowledgementConfirmsAcceptance() throws Exception { assertTrue(replicate(8, false)); }
}
