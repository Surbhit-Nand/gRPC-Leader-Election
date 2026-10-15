package com.cs324.client;

import com.cs324.progress.ProgressService;
import com.cs324.progress.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JobProgressStreamingTest {
    @Test void streamsActualCompletionsAndReplaysFastJobs() throws Exception {
        ProgressService service = new ProgressService();
        Server server = ServerBuilder.forPort(0).addService(service).build().start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        try {
            service.begin(42, Map.of(1, 3L, 2, 7L));
            BlockingQueue<JobProgress> updates = new LinkedBlockingQueue<>();
            CompletableFuture<Void> finished = new CompletableFuture<>();
            JobProgressServiceGrpc.newStub(channel).watchJob(WatchJobRequest.newBuilder().setJobId(42).build(), new StreamObserver<>() {
                public void onNext(JobProgress value) { updates.add(value); }
                public void onError(Throwable error) { finished.completeExceptionally(error); }
                public void onCompleted() { finished.complete(null); }
            });
            JobProgress initial = updates.poll(5, TimeUnit.SECONDS);
            assertNotNull(initial);
            assertEquals(0, completed(initial));
            assertFalse(initial.getFinished());
            service.complete(42, 1, true);
            JobProgress partial = updates.poll(5, TimeUnit.SECONDS);
            assertNotNull(partial);
            assertEquals(3, completed(partial));
            assertFalse(partial.getFinished());
            service.complete(42, 1, true); // Duplicate must not double count.
            assertEquals(3, completed(updates.poll(5, TimeUnit.SECONDS)));
            service.complete(42, 2, true);
            JobProgress last = updates.poll(5, TimeUnit.SECONDS);
            assertNotNull(last);
            assertEquals(10, completed(last));
            assertTrue(last.getFinished());
            finished.get(5, TimeUnit.SECONDS);
            var replay = JobProgressServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .watchJob(WatchJobRequest.newBuilder().setJobId(42).build());
            assertEquals(last, replay.next());
            assertFalse(replay.hasNext());

            service.begin(43, Map.of(1, 5L));
            service.complete(43, 1, false);
            JobProgress failed = JobProgressServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .watchJob(WatchJobRequest.newBuilder().setJobId(43).build()).next();
            assertTrue(failed.getFinished());
            assertEquals(0, completed(failed));
            assertEquals("Failed", failed.getWorkers(0).getStatus());
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private long completed(JobProgress value) {
        return value.getWorkersList().stream().mapToLong(WorkerProgress::getCompletedUnits).sum();
    }
}
