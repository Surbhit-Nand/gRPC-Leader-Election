package com.cs324.failover;

import com.cs324.bootstrap.*;
import com.cs324.computation.*;
import com.cs324.failover.proto.*;
import com.cs324.job.proto.*;
import com.cs324.registry.proto.*;
import com.cs324.worker.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class CheckpointReplicationTest {
    @Test void reorderedReplicationStillContainsTheCompleteJob() throws Exception {
        var pushes = new CopyOnWriteArrayList<ReplicateRequest>();
        var delivered = new CountDownLatch(1);
        Server peer = ServerBuilder.forPort(0).addService(new FailoverServiceGrpc.FailoverServiceImplBase() {
            @Override public void replicate(ReplicateRequest request, StreamObserver<ReplicateAck> reply) {
                pushes.add(request);
                reply.onNext(ReplicateAck.newBuilder().setAcknowledgedSequence(request.getEntriesList().stream()
                        .mapToLong(CheckpointEntry::getSequence).max().orElse(0)).build());
                reply.onCompleted();
                if (request.getEntriesList().stream().anyMatch(e -> e.hasFinished() && e.getFinished().getDelivered())) delivered.countDown();
            }
        }).build().start();
        Server bootstrap = ServerBuilder.forPort(0).addService(new BootstrapGrpcService(new BootstrapRegistryImpl())).build().start();
        var answer = new CompletableFuture<Object>();
        Server client = ServerBuilder.forPort(0).addService(new ClientResultService((id, value) -> answer.complete(value))).build().start();
        WorkerRuntime worker = new WorkerRuntime(1);
        ManagedChannel channel = null;
        try (RegistryClient directory = new RegistryClient("localhost", bootstrap.getPort())) {
            directory.registerWorker(1, "localhost", 0);
            directory.registerWorker(2, "localhost", 0);
            directory.publish(2, "localhost", 0, peer.getPort());
            worker.startProgressServer();
            worker.enableGrpcJobs("localhost", bootstrap.getPort(), "localhost", 0);
            worker.adoptGrpcCoordinator(1, 1L);
            channel = ManagedChannelBuilder.forAddress("localhost", worker.getProgressPort()).usePlaintext().build();
            var request = SubmitJobRequest.newBuilder().setJobId(1).setJobType("PRIMESUM")
                            .setPayload(ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 10)))
                            .setClient(ClientEndpoint.newBuilder().setHost("localhost").setPort(client.getPort())).build();
            var jobs = JobServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
            long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (true) {
                try { jobs.submitJob(request); break; }
                catch (StatusRuntimeException settling) {
                    if (settling.getStatus().getCode() != Status.Code.FAILED_PRECONDITION
                            || System.nanoTime() >= readyDeadline) throw settling;
                    Thread.sleep(25);
                }
            }
            assertEquals(17L, answer.get(5, TimeUnit.SECONDS));
            assertTrue(delivered.await(5, TimeUnit.SECONDS));
            // Different outbound RPCs may arrive in any order. Reproduce worst-case
            // ordering using actual messages emitted by the coordinator's sink.
            List<ReplicateRequest> reverse = new ArrayList<>(pushes);
            reverse.sort(Comparator.comparingLong((ReplicateRequest r) -> r.getEntriesList().stream()
                    .mapToLong(CheckpointEntry::getSequence).max().orElse(0)).reversed());
            CheckpointLog replica = new CheckpointLog(2);
            reverse.forEach(r -> r.getEntriesList().forEach(replica::apply));
            assertNotNull(replica.job(1));
            assertTrue(replica.job(1).finished(), "reordering must not lose the final result");
            assertTrue(replica.job(1).delivered());
            assertEquals(17L, replica.job(1).finalResult().getLongResult());
            assertEquals(0, replica.job(1).unfinished().size());
        } finally {
            worker.shutdown();
            if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            for (Server server : List.of(client, peer, bootstrap)) server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
