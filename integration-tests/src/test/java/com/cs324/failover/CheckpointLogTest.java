package com.cs324.failover;

import com.cs324.computation.ComputationCodec;
import com.cs324.failover.proto.CheckpointEntry;
import com.cs324.job.proto.ClientEndpoint;
import com.cs324.job.proto.SubmitJobRequest;
import com.cs324.job.proto.TaskPayload;
import com.cs324.job.proto.TaskResult;
import com.cs324.registry.proto.WorkerEndpoint;
import com.cs324.worker.WorkerTaskCalculator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verify duplicate, reordered and stale-term checkpoint handling.
class CheckpointLogTest {

    private static final long TERM_ONE = 1L;
    private static final long TERM_TWO = 2L;

    private static SubmitJobRequest job(int jobId) {
        return SubmitJobRequest.newBuilder()
                .setJobId(jobId)
                .setJobType("PRIMESUM")
                .setPayload(ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 100)))
                .setClient(ClientEndpoint.newBuilder().setHost("localhost").setPort(1199))
                .build();
    }

    private static WorkerEndpoint worker(int id) {
        return WorkerEndpoint.newBuilder().setWorkerId(id).setHost("localhost").setPort(4000 + id).build();
    }

    private static CheckpointLog logWithAcceptedJob(int jobId, int slices) {
        CheckpointLog log = new CheckpointLog(9);
        List<CheckpointSink.SliceSnapshot> snapshot = new java.util.ArrayList<>();
        for (int task = 1; task <= slices; task++) {
            snapshot.add(new CheckpointSink.SliceSnapshot(task,
                    ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 10)),
                    task == 1 ? worker(1) : worker(task), 1));
        }
        CheckpointEntry entry = log.acceptedEntry(job(jobId), snapshot, System.currentTimeMillis(),
                TERM_ONE, 1);
        assertTrue(log.apply(entry), "the accepted entry must be stored");
        return log;
    }

    @Test
    void anAcceptedEntryCarriesTheRequestAndEverySlice() {
        CheckpointLog log = logWithAcceptedJob(7, 3);
        JobCheckpoint job = log.job(7);
        assertNotNull(job);
        assertEquals("PRIMESUM", job.request().getJobType());
        assertEquals(3, job.slices().size());
        assertEquals(3, job.unfinished().size());
        assertEquals(1L, job.sequence());
    }

    @Test
    void aDuplicateEntryIsIgnoredRatherThanAppliedTwice() {
        CheckpointLog log = new CheckpointLog(9);
        List<CheckpointSink.SliceSnapshot> snapshot = List.of(
                new CheckpointSink.SliceSnapshot(1,
                        ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(1, 10)), worker(1), 1),
                new CheckpointSink.SliceSnapshot(2,
                        ComputationCodec.encode(new WorkerTaskCalculator.PrimeRange(11, 20)), worker(2), 1));
        CheckpointEntry entry = log.acceptedEntry(job(7), snapshot, System.currentTimeMillis(), TERM_ONE, 1);
        assertTrue(log.apply(entry));
        assertFalse(log.apply(entry), "replaying the same entry must change nothing");
        assertEquals(2, log.job(7).slices().size(), "the slice table is unchanged");
        assertEquals(1L, log.job(7).sequence(), "the sequence does not move for a duplicate");
    }

    @Test
    void aNewerSequenceWinsAndAnOlderOneAfterItIsRejected() {
        CheckpointLog log = logWithAcceptedJob(7, 1);
        CheckpointEntry reassigned = log.sliceEntry(7, 1, worker(4), 2, null, TERM_ONE, 1);
        assertTrue(log.apply(reassigned));
        assertEquals(2, log.job(7).slice(1).attempt());
        assertEquals(4, log.job(7).slice(1).owner().getWorkerId());

        CheckpointEntry stale = CheckpointEntry.newBuilder()
                .setJobId(7).setSequence(2).setTerm(TERM_ONE).setCoordinatorId(1)
                .setSlice(com.cs324.failover.proto.SliceUpdated.newBuilder()
                        .setTaskId(1).setAttempt(1).setOwner(worker(2)))
                .build();
        assertFalse(log.apply(stale), "an entry behind the current sequence is ignored");
        assertEquals(2, log.job(7).slice(1).attempt());
        assertEquals(4, log.job(7).slice(1).owner().getWorkerId());
    }

    @Test
    void aDeltaForAnUnknownJobIsDroppedBecauseItCarriesNoInput() {
        CheckpointLog log = new CheckpointLog(9);
        CheckpointEntry orphan = CheckpointEntry.newBuilder()
                .setJobId(42).setSequence(1).setTerm(TERM_ONE).setCoordinatorId(1)
                .setSlice(com.cs324.failover.proto.SliceUpdated.newBuilder().setTaskId(1).setAttempt(2))
                .build();
        assertFalse(log.apply(orphan), "a delta cannot rebuild a job whose inputs are unknown");
        assertNull(log.job(42));
    }

    @Test
    void anEntryFromADeposedCoordinatorIsRejected() {
        CheckpointLog log = logWithAcceptedJob(7, 1);
        // A newer term lands first, then the old coordinator tries to write again.
        CheckpointEntry newTerm = log.sliceEntry(7, 1, worker(5), 3, null, TERM_TWO, 5);
        assertTrue(log.apply(newTerm));
        CheckpointEntry fromZombie = CheckpointEntry.newBuilder()
                .setJobId(7).setSequence(99).setTerm(TERM_ONE).setCoordinatorId(1)
                .setSlice(com.cs324.failover.proto.SliceUpdated.newBuilder()
                        .setTaskId(1).setAttempt(9).setOwner(worker(1)))
                .build();
        assertFalse(log.apply(fromZombie), "a deposed coordinator must not resurrect its term");
        assertEquals(5, log.job(7).slice(1).owner().getWorkerId());
        assertEquals(3, log.job(7).slice(1).attempt());
    }

    @Test
    void aCompletedSliceKeepsItsResultSoRecoveryNeverRecomputesIt() {
        CheckpointLog log = logWithAcceptedJob(7, 2);
        TaskResult result = TaskResult.newBuilder().setJobId(7).setWorkerId(2)
                .setTaskId(2).setAttempt(1).setLongResult(1060L).build();
        assertTrue(log.apply(log.sliceEntry(7, 2, null, 0, result, TERM_ONE, 1)));

        JobCheckpoint job = log.job(7);
        assertEquals(1, job.unfinished().size(), "only the unfinished slice needs work");
        assertTrue(job.slice(2).completed());
        assertEquals(1060L, ((Number) com.cs324.computation.ComputationCodec.result(job.slice(2).result())).longValue());
    }

    @Test
    void aTombstoneStopsTheJobBeingRebuiltAndRemembersDelivery() {
        CheckpointLog log = logWithAcceptedJob(7, 2);
        TaskResult result = TaskResult.newBuilder().setJobId(7).setWorkerId(1).setLongResult(17L).build();
        assertTrue(log.apply(log.finishedEntry(7, result, false, TERM_ONE, 1)));
        assertTrue(log.job(7).finished());
        assertFalse(log.job(7).delivered(), "delivery was not confirmed yet");
        assertTrue(log.recoverable().stream().anyMatch(job -> job.jobId() == 7),
                "an undelivered result must still be replayable");

        assertTrue(log.apply(log.finishedEntry(7, result, true, TERM_ONE, 1)));
        assertTrue(log.job(7).delivered());
        assertTrue(log.recoverable().stream().noneMatch(job -> job.jobId() == 7),
                "a delivered result needs no takeover at all");
    }

    @Test
    void aSnapshotRebuildsTheSameStateOnAnotherReplica() {
        CheckpointLog source = logWithAcceptedJob(7, 2);
        source.apply(source.sliceEntry(7, 2, worker(3), 2, null, TERM_ONE, 1));
        source.apply(source.sliceEntry(7, 2, null, 0,
                TaskResult.newBuilder().setJobId(7).setWorkerId(3).setTaskId(2).setAttempt(2)
                        .setLongResult(5L).build(), TERM_ONE, 1));

        // A replica that starts empty folds only the snapshot it was handed.
        CheckpointLog replica = new CheckpointLog(4);
        for (CheckpointEntry entry : source.snapshot(7)) {
            assertTrue(replica.apply(entry));
        }
        JobCheckpoint copied = replica.job(7);
        assertEquals(2, copied.slices().size());
        assertEquals(3, copied.slice(2).owner().getWorkerId());
        assertEquals(2, copied.slice(2).attempt());
        assertTrue(copied.slice(2).completed());
        assertEquals(source.job(7).request(), copied.request());
    }

    @Test
    void sequencesContinueAboveWhatWasMergedSoATakeoverCannotRewind() {
        CheckpointLog replica = new CheckpointLog(4);
        replica.apply(logWithAcceptedJob(7, 1).snapshot(7).get(0));
        assertEquals(2L, replica.nextSequence(7), "the first write after a merge sits above it");
        assertEquals(1L, replica.nextSequence(99), "an unknown job starts its own sequence");
    }

    @Test
    void finishedSnapshotDoesNotConsumeTheNextDeliverySequence() {
        CheckpointLog source = logWithAcceptedJob(7, 1);
        TaskResult result = TaskResult.newBuilder().setJobId(7).setLongResult(17L).build();
        source.apply(source.finishedEntry(7, result, false, TERM_ONE, 1));
        CheckpointLog replica = new CheckpointLog(4);
        source.snapshot(7).forEach(replica::apply);
        assertEquals(source.job(7).sequence(), replica.job(7).sequence(),
                "fetching a snapshot must not invent a write sequence");
        var delivered = source.finishedEntry(7, result, true, TERM_ONE, 1);
        source.apply(delivered);
        assertTrue(replica.apply(delivered));
        assertTrue(replica.job(7).delivered());
    }

    @Test
    void deliveredTombstonesRemainVisibleToOtherReplicas() throws Exception {
        var worker = new com.cs324.worker.WorkerRuntime(4);
        try {
            CheckpointLog source = logWithAcceptedJob(7, 1);
            source.apply(source.finishedEntry(7, TaskResult.newBuilder().setJobId(7).setLongResult(17L).build(), true, TERM_ONE, 1));
            source.snapshot(7).forEach(worker.checkpointLog()::apply);
            assertFalse(worker.snapshots().isEmpty(), "a stale peer needs the terminal marker to avoid recovering this job");
            var replica = logWithAcceptedJob(7, 1);
            worker.snapshots().forEach(replica::apply);
            assertTrue(replica.job(7).finished());
            assertTrue(replica.job(7).delivered());
        } finally { worker.shutdown(); }
    }

    @Test
    void aClaimHandsTheJobToTheCoordinatorThatRebuiltIt() {
        CheckpointLog log = logWithAcceptedJob(7, 1);
        assertEquals(1, log.job(7).acceptedBy(), "the accepting coordinator owns the job");

        assertTrue(log.apply(log.claimEntry(7, 5, TERM_TWO)));
        assertEquals(5, log.job(7).acceptedBy(), "the takeover owns the job it rebuilt");
        assertEquals(2L, log.job(7).ownerTerm());

        // An older takeover cannot take the job back from the newer owner.
        assertFalse(log.apply(log.claimEntry(7, 6, TERM_ONE)), "an older term changes nothing");
        assertEquals(5, log.job(7).acceptedBy(), "an older term cannot steal the job");
    }

    @Test
    void aSnapshotTellsTheNextReplicaWhoOwnsTheJob() {
        CheckpointLog source = logWithAcceptedJob(7, 1);
        source.apply(source.claimEntry(7, 5, TERM_TWO));

        CheckpointLog replica = new CheckpointLog(4);
        source.snapshot(7).forEach(entry -> assertTrue(replica.apply(entry)));
        assertEquals(5, replica.job(7).acceptedBy(), "a survivor learns that the job was already rebuilt");
        assertEquals(2L, replica.job(7).ownerTerm());
    }

    @Test
    void aClaimForAnUnknownJobIsDroppedBecauseThereIsNothingToOwn() {
        CheckpointLog log = new CheckpointLog(9);
        assertNull(log.claimEntry(42, 5, TERM_TWO), "there is no job to claim");
        assertNull(log.job(42));
    }

    @Test
    void entriesWithoutARequestOrPayloadShapeAreRejected() {
        CheckpointLog log = new CheckpointLog(9);
        assertFalse(log.apply(CheckpointEntry.getDefaultInstance()), "an empty entry changes nothing");
        CheckpointEntry noRequest = CheckpointEntry.newBuilder().setJobId(7).setSequence(1).setTerm(TERM_ONE)
                .setAccepted(com.cs324.failover.proto.JobAccepted.newBuilder()
                        .setAcceptedAtMillis(System.currentTimeMillis()))
                .build();
        assertFalse(log.apply(noRequest), "an acceptance with no request cannot rebuild anything");
    }
}
