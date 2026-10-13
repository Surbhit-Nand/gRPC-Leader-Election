package com.cs324.test;


import com.cs324.common.Constants;
import com.cs324.worker.WorkerRemoteImpl;
import com.cs324.worker.WorkerTaskCalculator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.lang.reflect.Method;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// End-to-end dispatch over real RMI: worker 1 leads a 3-worker ring, splits each job across
// itself and its peers, then hands the aggregate to a recording client. JAC is asserted
// coordinator-side, because only the coordinator's own peer assignments count.
class WorkerDispatchTest {

    private static final int TEST_RMI_PORT = 19808;
    private static final String HOST = "localhost";
    private static final String CLIENT_NAME = "Client";
    private static final long AWAIT_TIMEOUT_MS = 10_000;

    // One registry for the whole class; each test rebinds fresh workers into it.
    private static Registry registry;

    private WorkerRemoteImpl w1;
    private WorkerRemoteImpl w2;
    private WorkerRemoteImpl w3;
    private RecordingClientRemote client;

    @BeforeAll
    static void startRegistry() throws RemoteException {
        registry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @BeforeEach
    void setUp() throws Exception {
        // Fresh workers per test: counters, neighbours and term budgets must not leak.
        w1 = new WorkerRemoteImpl(1);
        w2 = new WorkerRemoteImpl(2);
        w3 = new WorkerRemoteImpl(3);
        client = new RecordingClientRemote();

        registry.rebind("Worker1", w1);
        registry.rebind("Worker2", w2);
        registry.rebind("Worker3", w3);
        registry.rebind(CLIENT_NAME, client);

        // Full ring so the coordinator knows every participant.
        w1.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
        w1.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
        w2.connectAsNeighbour(1, HOST, TEST_RMI_PORT);
        w2.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
        w3.connectAsNeighbour(1, HOST, TEST_RMI_PORT);
        w3.connectAsNeighbour(2, HOST, TEST_RMI_PORT);

        // Elect worker 1 as the coordinator on every node.
        w1.receiveCoordinatorMessage(1, 100L);
        w2.receiveCoordinatorMessage(1, 101L);
        w3.receiveCoordinatorMessage(1, 102L);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Clear every binding, so the next test looks up nothing left over from this one.
        for (String name : registry.list()) {
            try {
                registry.unbind(name);
            } catch (Exception ignored) {
            }
        }
        // Stop the pools before unexporting, so no task thread outlives its worker.
        for (WorkerRemoteImpl worker : new WorkerRemoteImpl[]{w1, w2, w3}) {
            if (worker != null) {
                worker.shutdown();
            }
        }
        unexportQuietly(w1);
        unexportQuietly(w2);
        unexportQuietly(w3);
        unexportQuietly(client);
    }

    // Unexport is best effort here, because some tests unexport a worker themselves.
    private void unexportQuietly(Remote remoteObject) {
        if (remoteObject == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
        }
    }

    // Results arrive on a pool thread, so every assertion has to wait for the callback.
    private void awaitResult(int jobId) throws Exception {
        // Deadline rather than a fixed sleep: RMI delivery time is not deterministic.
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (client.received(jobId)) {
                return;
            }
            Thread.sleep(20);
        }
        fail("job " + jobId + " never returned within " + AWAIT_TIMEOUT_MS + "ms");
    }

    @Test
    void maxIsCalculatedAcrossSplitPayloads() throws Exception {
        // Three-way split: the coordinator's own slice and both peer slices must recombine.
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);
        assertEquals(22, client.resultFor(1));
    }

    @Test
    void primeSumIsAggregatedAcrossRanges() throws Exception {
        // PRIMESUM reduces to a sum, so the total is only right if every range was counted.
        w1.submitJob(2, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, 20),
                HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(2);
        assertEquals(77L, client.resultFor(2));
    }

    @Test
    void primeCountIsAggregatedAcrossLists() throws Exception {
        // PRIMECOUNT adds per-participant counts, so a dropped slice shows up short.
        w1.submitJob(3, "PRIMECOUNT", new int[]{2, 4, 6, 8, 10, 11, 13},
                HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(3);
        assertEquals(3, client.resultFor(3));
    }

    @Test
    void coordinatorJacCountsOnlyAssignmentsToOtherWorkers() throws Exception {
        // The JAC is the election input, so it must count peer work and nothing else.
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);

        // Two of the three slices went to workers 2 and 3.
        assertEquals(2, w1.getJobAllocationCounter(),
                "coordinator JAC counts each assignment to another worker");
        assertEquals(0, w2.getJobAllocationCounter(),
                "a receiving worker never counts assigned work");
        assertEquals(0, w3.getJobAllocationCounter(),
                "a receiving worker never counts assigned work");
    }

    @Test
    void nonCoordinatorRejectsJobs() throws Exception {
        // Leadership is checked before any split, so a non-coordinator delivers nothing.
        RemoteException thrown = assertThrows(RemoteException.class, () ->
                w2.submitJob(9, "MAX", new int[]{1, 2, 3}, HOST, TEST_RMI_PORT, CLIENT_NAME));
        assertEquals(0, client.callCount(),
                "no result may be delivered for a job the coordinator rejected");
    }

    @Test
    void deadParticipantSliceIsComputedLocally() throws Exception {
        // Sanity: a full job first, so the participant set is established.
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);
        assertEquals(22, client.resultFor(1));

        // Worker 2 "dies" (unexported) after the first job.
        UnicastRemoteObject.unexportObject(w2, true);

        // The dead worker's slice is unreachable, so only a local recompute can finish the job.
        w1.submitJob(5, "MAX", new int[]{30, 7, 1, 300, 12, 2}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(5);

        assertEquals(300, client.resultFor(5),
                "a dead participant's slice must be recomputed by the coordinator so the "
                        + "aggregate stays correct");
    }

    @Test
    void coordinatorKeepsLeadingUntilTheFifthJobOfItsTerm() throws Exception {
        // A term is JOBS_PER_TERM (5) client jobs, not 5 task assignments. Counting the
        // assignments instead rotated the leader after only 2 jobs.
        for (int j = 1; j <= 4; j++) {
            w1.submitJob(200 + j, "MAX", new int[]{j, j + 7, 42}, HOST, TEST_RMI_PORT, CLIENT_NAME);
            awaitResult(200 + j);
        }

        assertEquals(1, w1.getCurrentCoordinatorId(),
                "the coordinator must still be leading during its first four jobs of the term");
        assertEquals(4, w1.getJobsCoordinatedInTerm(),
                "each accepted job counts once against the term, regardless of how many workers it is split across");
        assertEquals(8, w1.getJobAllocationCounter(),
                "the JAC still counts every assignment issued to another worker");
    }

    @Test
    void coordinatorRotatesAfterItsTermTrigger() throws Exception {
        // Everyone starts on JAC 0; after 5 jobs w1 holds 10 while w2 and w3 hold 0, so the
        // lowest-JAC rule must hand leadership to worker 3.
        for (int j = 1; j <= 5; j++) {
            w1.submitJob(100 + j, "MAX", new int[]{j, j + 7, 42}, HOST, TEST_RMI_PORT, CLIENT_NAME);
            awaitResult(100 + j);
        }

        // Rotation runs on the finishing job's pool thread, so poll instead of reading once.
        final long deadline = System.currentTimeMillis() + 8_000;
        while (System.currentTimeMillis() < deadline) {
            if (w1.getCurrentCoordinatorId() != 1) {
                break;
            }
            Thread.sleep(50);
        }

        assertEquals(3, w1.getCurrentCoordinatorId(),
                "after JOBS_PER_TERM client jobs the coordinator must rotate to the "
                        + "lowest-JAC worker (w3), got coordinator=" + w1.getCurrentCoordinatorId());
        assertEquals(10, w1.getJobAllocationCounter(),
                "the departed coordinator keeps the JAC it earned during its term");
    }

    @Test
    void aTermRefusesASixthJobWhileItsLastSlotIsStillBeingDispatched() throws Exception {
        // Doubles record work and never report, so the term cannot rotate out mid-test.
        BlockingAssignWorkerRemote slow2 = installBlockingParticipant(2);
        installBlockingParticipant(3);

        // Four accepted jobs leave the term exactly one slot short of its budget.
        for (int job = 401; job <= 404; job++) {
            w1.submitJob(job, "MAX", new int[]{job, job + 1, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        }
        assertEquals(4, w1.getJobsCoordinatedInTerm(),
                "each accepted job takes exactly one slot of the term's budget");

        // The fifth job claims the last slot then parks inside dispatch: budget spent, not
        // yet reset. A sixth concurrent submission used to slip straight through this window.
        slow2.blockJob(405);
        Thread fifth = new Thread(() -> {
            try {
                w1.submitJob(405, "MAX", new int[]{405, 406, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
            } catch (RemoteException ignored) {
            }
        }, "fifth-submit");
        fifth.start();
        awaitTrue(() -> w1.getJobsCoordinatedInTerm() == 5,
                "the fifth job should have claimed the term's last slot");

        RemoteException refusal = assertThrows(RemoteException.class, () ->
                        w1.submitJob(406, "MAX", new int[]{406, 407, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME),
                "a sixth job must not be accepted into a term that has already served its budget");
        assertTrue(String.valueOf(refusal.getMessage()).contains("term"),
                "the refusal should say the term is spent, got: " + refusal.getMessage());
        assertEquals(5, w1.getJobsCoordinatedInTerm(),
                "a refused job must hand its slot back instead of consuming one");

        slow2.release();
        fifth.join(AWAIT_TIMEOUT_MS);
    }

    @Test
    void concurrentSubmissionsNeverOverfillATerm() throws Exception {
        // Doubles that never report, so no job dispatched in this test can ever finish.
        BlockingAssignWorkerRemote quiet2 = installBlockingParticipant(2);
        installBlockingParticipant(3);

        long electionsBefore = electionCounterValue(w1);
        final int submissions = 6;
        // One gate for all callers, so the six submissions genuinely overlap.
        CountDownLatch armed = new CountDownLatch(submissions);
        CountDownLatch fire = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        List<Throwable> refused = Collections.synchronizedList(new ArrayList<>());
        List<Thread> callers = new ArrayList<>();

        for (int j = 0; j < submissions; j++) {
            final int jobId = 500 + j;
            Thread caller = new Thread(() -> {
                armed.countDown();
                try {
                    fire.await();
                    w1.submitJob(jobId, "MAX", new int[]{jobId, jobId + 1, 7},
                            HOST, TEST_RMI_PORT, CLIENT_NAME);
                    accepted.incrementAndGet();
                } catch (Throwable failure) {
                    refused.add(failure);
                }
            }, "submit-" + jobId);
            callers.add(caller);
            caller.start();
        }

        assertTrue(armed.await(10, TimeUnit.SECONDS), "every submission should be waiting at the gate");
        fire.countDown();
        for (Thread caller : callers) {
            caller.join(AWAIT_TIMEOUT_MS);
        }

        // No job ever completes, so a term can end only by reservation, and the election
        // count measures terms spent directly: no term may hold more than JOBS_PER_TERM jobs.
        long elections = electionCounterValue(w1) - electionsBefore;
        assertTrue(elections >= 1,
                "a spent term must trigger an election; the counter moved by " + elections);
        assertEquals(elections, countCalls(quiet2.calls(), "election|"),
                "every election started should have reached the participants exactly once; recorded on "
                        + "worker 2: " + quiet2.calls());
        assertTrue(accepted.get() <= 5L * elections,
                "each term accepts at most 5 jobs, so " + accepted.get() + " accepted jobs need at least "
                        + ((accepted.get() + 4) / 5) + " elections, but only " + elections
                        + " were started");
        assertEquals(submissions, accepted.get() + refused.size(),
                "every submission must be either accepted or refused, never silently dropped");
    }

    @Test
    void aTermRefusesAJobWhileLeadershipIsBeingHandedOver() throws Exception {
        // These participants record an election but never echo it, so no wave ever concludes.
        BlockingAssignWorkerRemote stalled2 = installBlockingParticipant(2);
        installBlockingParticipant(3);

        // Five jobs spend the term and the fifth starts the handover. The wave stalls, so this
        // node stays named coordinator while its budget is already zero: the only gate on
        // submitJob used to be the coordinator id, and that still read "me".
        for (int job = 601; job <= 605; job++) {
            w1.submitJob(job, "MAX", new int[]{job, job + 1, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        }

        assertEquals(0, w1.getJobsCoordinatedInTerm(),
                "the fifth job spends the term and resets the budget");
        assertEquals(1, w1.getCurrentCoordinatorId(),
                "the wave has stalled, so this node is still named coordinator");
        assertTrue(countCalls(stalled2.calls(), "election|") >= 1,
                "the fifth job should have started the handover election; recorded on worker 2: "
                        + stalled2.calls());

        RemoteException refusal = assertThrows(RemoteException.class, () ->
                        w1.submitJob(606, "MAX", new int[]{606, 607, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME),
                "a job arriving mid-handover must not be accepted into the term that has just ended");
        assertTrue(String.valueOf(refusal.getMessage()).contains("handing over"),
                "the refusal should say a handover is in progress, got: " + refusal.getMessage());
        assertEquals(0, w1.getJobsCoordinatedInTerm(),
                "a refused job must not consume any of the incoming term's budget");
    }

    @Test
    void aJoiningWorkerDoesNotResetTheServingCoordinatorsTerm() throws Exception {
        // Four accepted jobs: this term is part-spent, which is exactly the state a second
        // budget must not be handed out in.
        for (int job = 701; job <= 704; job++) {
            w1.submitJob(job, "MAX", new int[]{job, job + 1, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        }
        assertEquals(4, w1.getJobsCoordinatedInTerm(), "four jobs leave this term part-spent");

        // A joiner adopts the leader its peer already knows, through the local-only
        // adoptCoordinator. Re-broadcasting it instead would reach the serving coordinator
        // under a fresh id it cannot tell from a new term: budget reset, nine jobs in one term.
        WorkerRemoteImpl joiner = new WorkerRemoteImpl(9);
        registry.rebind("Worker9", joiner);
        joiner.connectAsNeighbour(1, HOST, TEST_RMI_PORT);
        try {
            joiner.adoptCoordinator(1, 900L);

            assertEquals(1, joiner.getCurrentCoordinatorId(),
                    "the joiner should adopt the coordinator it was told about");
            assertEquals(4, w1.getJobsCoordinatedInTerm(),
                    "a joiner adopting the current coordinator must not restart the term it is joining");
        } finally {
            // The joiner was never part of the ring, so it is torn down on its own.
            joiner.shutdown();
            unexportQuietly(joiner);
        }
    }

    @Test
    void adoptingACoordinatorIsNotRelayedToNeighbours() throws Exception {
        // A joiner whose only neighbour is a recording peer, so a relay would show in its log.
        RecordingWorkerRemote peer = new RecordingWorkerRemote(8);
        registry.rebind("Worker8", peer);

        WorkerRemoteImpl joiner = new WorkerRemoteImpl(9);
        registry.rebind("Worker9", joiner);
        joiner.connectAsNeighbour(8, HOST, TEST_RMI_PORT);
        try {
            joiner.adoptCoordinator(8, 900L);

            assertEquals(8, joiner.getCurrentCoordinatorId(), "the joiner should know the coordinator");
            assertEquals(0, countCalls(peer.calls(), "coordinator|"),
                    "adoption is local: the peer it learned from already knows this coordinator, so "
                            + "relaying would only spread an announcement nobody needs; recorded on worker 8: "
                            + peer.calls());
        } finally {
            joiner.shutdown();
            unexportQuietly(joiner);
            unexportQuietly(peer);
        }
    }

    @Test
    void aReservationRacingARotationKeepsTheTermBudgetInRange() throws Exception {
        // reserveTermSlot and maybeRotateCoordinator are private, so both are driven directly.
        // The CAS keeps the budget inside [0, JOBS_PER_TERM]: a reservation only moves a value
        // it has already checked to be under the cap, and a refusal has nothing to undo - so
        // no late decrement can land after a rotation has already reset the counter to zero.
        // Enough racers and iterations to interleave reservations and resets many times.
        final int reservers = 4;
        final int rotators = 2;
        final int iterations = 4000;

        setTermCount(w1, 0);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger highest = new AtomicInteger(0);
        AtomicInteger lowest = new AtomicInteger(Constants.JOBS_PER_TERM);

        // Reads the raw budget continuously: an out-of-range value need only be seen once.
        Thread watcher = new Thread(() -> {
            while (!stop.get()) {
                int seen = termCountUnchecked(w1);
                highest.accumulateAndGet(seen, Math::max);
                lowest.accumulateAndGet(seen, Math::min);
            }
        }, "budget-watcher");
        watcher.setDaemon(true);
        watcher.start();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < reservers; i++) {
            Thread thread = new Thread(() -> {
                for (int n = 0; n < iterations; n++) {
                    try {
                        reserveSlot(w1, 9000 + n);
                    } catch (Exception refused) {
                        // A full term refuses; that is an expected outcome here.
                    }
                }
            }, "reserve-" + i);
            threads.add(thread);
            thread.start();
        }
        for (int i = 0; i < rotators; i++) {
            Thread thread = new Thread(() -> {
                for (int n = 0; n < iterations; n++) {
                    try {
                        rotateIfTermSpent(w1);
                    } catch (Exception ignored) {
                        // Reported through the budget, which is what is asserted.
                    }
                }
            }, "rotate-" + i);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join(AWAIT_TIMEOUT_MS);
        }
        stop.set(true);
        watcher.join(AWAIT_TIMEOUT_MS);

        int high = highest.get();
        int low = lowest.get();
        assertTrue(low >= 0,
                "the term budget was observed at " + low + ", below zero. A refused reservation's late "
                        + "decrement has to land after a rotation that already reset the counter, and from a "
                        + "negative budget a term that already served its share keeps admitting jobs");
        assertTrue(high <= Constants.JOBS_PER_TERM,
                "the term budget was observed at " + high + ", above the cap of " + Constants.JOBS_PER_TERM
                        + ". Publishing an over-cap value means the counter was moved before it was checked, "
                        + "so concurrent readers see a spent term as having room left");
    }

    // Reads the term budget without the checked reflection of termCount().
    private static int termCountUnchecked(WorkerRemoteImpl worker) {
        try {
            return termCount(worker);
        } catch (Exception unreadable) {
            return 0;
        }
    }

    @Test
    void aSpentTermStartsExactlyOneElectionHoweverManyThreadsNoticeIt() throws Exception {
        // maybeRotateCoordinator is private and the race is only reachable from inside the
        // node, so it is driven directly: the term is marked spent and several threads notice.
        // Resetting with get()-then-set() let two threads both see a spent term and both start
        // an election; each computes its own winner from the JACs it sees, so two waves for
        // one term can settle on different leaders and leave the cluster disagreeing.
        // Repeated rounds, because a single round can pass by luck.
        final int rounds = 500;
        final int racers = 8;
        long electionsBefore = electionCounterValue(w1);

        for (int round = 0; round < rounds; round++) {
            markTermSpent(w1);
            CountDownLatch armed = new CountDownLatch(racers);
            CountDownLatch fire = new CountDownLatch(1);
            List<Thread> threads = new ArrayList<>();
            for (int racer = 0; racer < racers; racer++) {
                Thread thread = new Thread(() -> {
                    armed.countDown();
                    try {
                        fire.await();
                        rotateIfTermSpent(w1);
                    } catch (Exception ignored) {
                        // Reported through the election counter, which is what is asserted.
                    }
                }, "rotate-" + round + "-" + racer);
                threads.add(thread);
                thread.start();
            }
            assertTrue(armed.await(10, TimeUnit.SECONDS), "every racer should be waiting at the gate");
            fire.countDown();
            for (Thread thread : threads) {
                thread.join(AWAIT_TIMEOUT_MS);
            }

            long spent = electionCounterValue(w1) - electionsBefore;
            assertEquals(round + 1L, spent,
                    "round " + round + ": " + racers + " threads found the term spent, but a term may only be "
                            + "handed over once - " + (spent - (round + 1L)) + " extra election(s) were started, so "
                            + "two threads both won the reset and are running rival elections that can disagree on "
                            + "the leader");
        }
    }

    // Puts the node's term budget into the spent state a rotation reacts to.
    private static void markTermSpent(WorkerRemoteImpl worker) throws Exception {
        setTermCount(worker, Constants.JOBS_PER_TERM);
    }

    // Writes the term budget directly, as a served job's reservation would.
    private static void setTermCount(WorkerRemoteImpl worker, int jobs) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("termJobsCoordinated");
        field.setAccessible(true);
        ((AtomicInteger) field.get(worker)).set(jobs);
    }

    // Reflective read of the same budget, for the assertions that must know its value.
    private static int termCount(WorkerRemoteImpl worker) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("termJobsCoordinated");
        field.setAccessible(true);
        return ((AtomicInteger) field.get(worker)).get();
    }

    // Invokes the private reservation, which submits no work of its own.
    private static boolean reserveSlot(WorkerRemoteImpl worker, int jobId) throws Exception {
        Method method = WorkerRemoteImpl.class.getDeclaredMethod("reserveTermSlot", int.class);
        method.setAccessible(true);
        return (boolean) method.invoke(worker, jobId);
    }

    // Invokes the private rotation hook directly, as a finishing job would.
    private static void rotateIfTermSpent(WorkerRemoteImpl worker) throws Exception {
        Method method = WorkerRemoteImpl.class.getDeclaredMethod("maybeRotateCoordinator");
        method.setAccessible(true);
        method.invoke(worker);
    }

    // Rebinds a participant as a worker that records but never reports, so jobs dispatched
    // to it never complete. connectAsNeighbour overwrites by id, which is what lets the
    // coordinator pick the double up as its participant.
    private BlockingAssignWorkerRemote installBlockingParticipant(int workerId) throws Exception {
        BlockingAssignWorkerRemote participant = new BlockingAssignWorkerRemote(workerId);
        registry.rebind("Worker" + workerId, participant);
        w1.connectAsNeighbour(workerId, HOST, TEST_RMI_PORT);
        return participant;
    }

    // Reads the private election counter, i.e. how many elections were started.
    private static long electionCounterValue(WorkerRemoteImpl worker) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("electionCounter");
        field.setAccessible(true);
        return ((AtomicLong) field.get(worker)).get();
    }

    // Prefix-matched, so one message kind's calls can never be counted as another's.
    private static long countCalls(List<String> calls, String prefix) {
        return calls.stream().filter(call -> call.startsWith(prefix)).count();
    }

    // Polls to a deadline, so a failing expectation fails the test instead of hanging it.
    private static void awaitTrue(BooleanSupplier condition, String failureMessage) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        fail(failureMessage);
    }
}