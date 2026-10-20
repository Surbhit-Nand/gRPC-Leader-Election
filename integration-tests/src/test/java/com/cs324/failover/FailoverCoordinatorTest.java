package com.cs324.failover;

import com.cs324.common.WorkerEvent;
import com.cs324.computation.ComputationCodec;
import com.cs324.failover.proto.FetchReply;
import com.cs324.job.proto.ClientEndpoint;
import com.cs324.job.proto.SubmitJobRequest;
import com.cs324.job.proto.TaskResult;
import com.cs324.registry.proto.WorkerEndpoint;
import com.cs324.worker.WorkerTaskCalculator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Use scripted peers to verify takeover ownership, deduplication and reuse of completed results.
class FailoverCoordinatorTest {

    private static final long TERM = 4L;
    private static final int SELF = 2;

    // Peers the takeover can see, what each answers with, and which are simply gone.
    private static final class FakeSource implements CheckpointSource {
        private final List<WorkerEndpoint> live = new ArrayList<>();
        private final Map<Integer, FetchReply> answers = new LinkedHashMap<>();
        private final java.util.Set<Integer> unreachable = new java.util.HashSet<>();

        FakeSource alive(int... ids) {
            for (int id : ids) {
                live.add(WorkerEndpoint.newBuilder().setWorkerId(id).setHost("localhost")
                        .setGrpcPort(5000 + id).build());
            }
            return this;
        }

        // Registered but not answering: what a crashed worker looks like until it exits.
        FakeSource unreachable(int... ids) {
            alive(ids);
            for (int id : ids) {
                unreachable.add(id);
            }
            return this;
        }

        FakeSource holds(int id, CheckpointLog replica) {
            List<com.cs324.failover.proto.CheckpointEntry> entries = new ArrayList<>();
            for (int jobId : replica.jobIds()) {
                entries.addAll(replica.snapshot(jobId));
            }
            answers.put(id, FetchReply.newBuilder().setReplicaId(id).addAllEntries(entries).build());
            return this;
        }

        @Override
        public List<WorkerEndpoint> readyPeers() {
            return List.copyOf(live);
        }

        @Override
        public FetchReply fetch(WorkerEndpoint peer) {
            return unreachable.contains(peer.getWorkerId())
                    ? null
                    : answers.getOrDefault(peer.getWorkerId(), FetchReply.getDefaultInstance());
        }
    }

    private final List<RestoredJob> restored = new ArrayList<>();
    private final List<RestoredJob> replayed = new ArrayList<>();
    private final List<String> events = new ArrayList<>();

    private FailoverCoordinator coordinator(CheckpointSource source) {
        return new FailoverCoordinator(SELF, new CheckpointLog(SELF), source,
                (job, remaining) -> {
                    restored.add(job);
                    return true;
                },
                job -> {
                    replayed.add(job);
                    return true;
                },
                () -> TERM, 600_000L, events::add);
    }

    private static SubmitJobRequest job(int jobId) {
        return SubmitJobRequest.newBuilder()
                .setJobId(jobId)
                .setJobType("PRIMESUM")
                .setPayload(ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 100)))
                .setClient(ClientEndpoint.newBuilder().setHost("localhost").setPort(1199))
                .build();
    }

    private static WorkerEndpoint worker(int id) {
        return WorkerEndpoint.newBuilder().setWorkerId(id).setHost("localhost").setGrpcPort(5000 + id).build();
    }

    // A replica of a two-slice job accepted by acceptedBy.
    private static CheckpointLog replicaOf(int jobId, int acceptedBy) {
        CheckpointLog log = new CheckpointLog(5);
        log.apply(log.acceptedEntry(job(jobId), List.of(
                new CheckpointSink.SliceSnapshot(1,
                        ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 50)), worker(acceptedBy), 1),
                new CheckpointSink.SliceSnapshot(2,
                        ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(51, 100)), worker(3), 1)),
                System.currentTimeMillis(), 1L, acceptedBy));
        return log;
    }

    @Test
    void aJobOwnedByAWorkerThatIsRegisteredButSilentIsStillTakenOver() {
        // A registered worker may be unreachable; recovery must probe its liveness.
        CheckpointLog replica = replicaOf(1, 1);
        FakeSource source = new FakeSource().alive(3, 4).unreachable(1).holds(3, replica);

        assertEquals(1, coordinator(source).takeOver());
        assertEquals(1, restored.size());
    }

    @Test
    void aJobWhoseAcceptingCoordinatorIsStillAliveIsLeftAlone() {
        // Leadership rotation must not steal unfinished jobs from a live coordinator.
        CheckpointLog replica = replicaOf(1, 1);
        int rebuilt = coordinator(new FakeSource().alive(1, 3, 4).holds(3, replica)).takeOver();

        assertEquals(0, rebuilt);
        assertTrue(restored.isEmpty(), "a live owner's job must never be rebuilt by the new leader");
    }

    @Test
    void aJobWhoseOwnerDiedIsRebuiltWithTheDeadSlicesReattempted() {
        // Worker 1 accepted the job and owned slice 1; worker 3 still holds slice 2.
        CheckpointLog replica = replicaOf(1, 1);
        int rebuilt = coordinator(new FakeSource().alive(3, 4).holds(3, replica)).takeOver();

        assertEquals(1, rebuilt);
        assertEquals(1, restored.size());
        RestoredJob job = restored.get(0);
        assertEquals(1, job.jobId());
        RestoredJob.RestoredSlice first = job.slices().get(0);
        RestoredJob.RestoredSlice second = job.slices().get(1);
        // Increase the attempt when reassigning the failed coordinator slice to reject late results.
        assertEquals(SELF, first.owner().getWorkerId());
        assertEquals(2, first.attempt());
        // The live worker keeps its slice and its attempt: it still holds that work.
        assertEquals(3, second.owner().getWorkerId());
        assertEquals(1, second.attempt());
    }

    @Test
    void aSliceThatAlreadyAnsweredIsNeverRecomputed() {
        // Adding both copies of a slice is exactly how a sum silently goes wrong.
        CheckpointLog replica = replicaOf(1, 1);
        replica.apply(replica.sliceEntry(1, 2, null, 0,
                TaskResult.newBuilder().setJobId(1).setWorkerId(3).setTaskId(2).setAttempt(1)
                        .setLongResult(1060L).build(), 1L, 1));
        coordinator(new FakeSource().alive(3, 4).holds(3, replica)).takeOver();

        RestoredJob job = restored.get(0);
        assertTrue(job.slices().get(1).completed(), "a finished slice keeps its answer");
        assertEquals(1060L, ((Number) ComputationCodec.result(job.slices().get(1).result())).longValue());
        assertNull(job.slices().get(0).result(), "the unfinished slice has no answer yet");
    }

    @Test
    void takeoverIsIdempotentPerJobAndTerm() {
        CheckpointLog replica = replicaOf(1, 1);
        FailoverCoordinator coordinator = coordinator(new FakeSource().alive(3, 4).holds(3, replica));

        assertEquals(1, coordinator.takeOver(), "the first takeover rebuilds the job");
        assertEquals(0, coordinator.takeOver(), "a second trigger in the same term must not rebuild it");
        assertEquals(0, coordinator.takeOver());
        assertEquals(1, restored.size());
    }

    @Test
    void aFinishedAndDeliveredJobIsIgnoredEntirely() {
        CheckpointLog replica = replicaOf(1, 1);
        TaskResult result = TaskResult.newBuilder().setJobId(1).setWorkerId(1).setLongResult(1060L).build();
        replica.apply(replica.finishedEntry(1, result, true, 1L, 1));
        int rebuilt = coordinator(new FakeSource().alive(3, 4).holds(3, replica)).takeOver();

        assertEquals(0, rebuilt);
        assertTrue(restored.isEmpty());
        assertTrue(replayed.isEmpty(), "a delivered answer is never sent twice");
    }

    @Test
    void aFinishedJobWhoseDeliveryIsUnknownHasItsAnswerReplayed() {
        CheckpointLog replica = replicaOf(1, 1);
        TaskResult result = TaskResult.newBuilder().setJobId(1).setWorkerId(1).setLongResult(1060L).build();
        replica.apply(replica.finishedEntry(1, result, false, 1L, 1));
        int rebuilt = coordinator(new FakeSource().alive(3, 4).holds(3, replica)).takeOver();

        assertEquals(0, rebuilt, "there is no work left to rebuild");
        assertEquals(1, replayed.size(), "the client is owed an answer it never confirmed");
        assertEquals(1, replayed.get(0).jobId());
    }

    @Test
    void failedReplayRemainsUndeliveredAndCanBeRetried() {
        CheckpointLog log = replicaOf(1, 1);
        log.apply(log.finishedEntry(1, TaskResult.newBuilder().setJobId(1).setLongResult(1060L).build(), false, 1, 1));
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        FailoverCoordinator coordinator = new FailoverCoordinator(SELF, log, new FakeSource(),
                (job, remaining) -> false, job -> calls.incrementAndGet() > 1,
                () -> TERM, 600_000L, events::add);
        coordinator.takeOver();
        assertFalse(log.job(1).delivered(), "an attempted callback is not an acknowledgement");
        coordinator.takeOver();
        assertTrue(log.job(1).delivered());
        assertEquals(2, calls.get());
    }

    @Test
    void takeoverMergesEveryReplicaBeforeDeciding() {
        // Merge both replicas before recovery so completed slices from either peer are retained.
        CheckpointLog withResult = replicaOf(1, 1);
        withResult.apply(withResult.sliceEntry(1, 2, null, 0,
                TaskResult.newBuilder().setJobId(1).setWorkerId(3).setTaskId(2).setAttempt(1)
                        .setLongResult(1060L).build(), 1L, 1));
        CheckpointLog withoutResult = replicaOf(1, 1);

        FakeSource source = new FakeSource().alive(3, 4).holds(3, withResult).holds(4, withoutResult);
        coordinator(source).takeOver();

        assertTrue(restored.get(0).slices().get(1).completed(),
                "the replica that knew the answer wins over the one that did not");
    }

    @Test
    void theFeedNamesBothEndsOfTheFailover() {
        CheckpointLog replica = replicaOf(1, 1);
        coordinator(new FakeSource().alive(3, 4).holds(3, replica)).takeOver();

        assertTrue(events.stream().anyMatch(line -> line.startsWith(WorkerEvent.FAILOVER_STARTED)),
                "the feed must show the failover starting");
        assertTrue(events.stream().anyMatch(line -> line.startsWith(WorkerEvent.JOB_RECOVERED)),
                "the feed must name the job that came back");
        assertTrue(events.stream().anyMatch(line -> line.startsWith(WorkerEvent.FAILOVER_COMPLETED)),
                "the feed must show the failover finishing");
        assertNotNull(restored.get(0).request().getClient(), "the restored job keeps its client endpoint");
    }
}
