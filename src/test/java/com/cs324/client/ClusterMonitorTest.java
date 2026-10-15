package com.cs324.client;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.common.WorkerEvent;
import com.cs324.common.WorkerRemote;

// Headless tests for the cluster-activity view: snapshot polling plus
// transition-derived events, with map-backed workers instead of RMI.
class ClusterMonitorTest {

    // Configurable stand-in: the shared FakeWorkerRemote is fixed
    // (empty neighbours, JAC 0), which cannot express rewirings or assignments.
    static class ConfigurableWorker extends UnicastRemoteObject implements WorkerRemote {
        int id;
        int jac;
        int view;
        Set<Integer> neighbours = Set.of();
        List<WorkerEvent> script = List.of();

        ConfigurableWorker(int id, int jac, int view, Set<Integer> neighbours) throws RemoteException {
            super();
            this.id = id;
            this.jac = jac;
            this.view = view;
            this.neighbours = neighbours;
        }

        @Override public int getId() { return id; }
        @Override public int getJobAllocationCounter() { return jac; }
        @Override public int getCurrentCoordinatorId() { return view; }
        @Override public long startElection() { return 0L; }
        @Override public void ping() {}
        @Override public void submitJob(int jobId, String jobType, Object payload,
                String clientHost, int clientPort, String clientName) {}
        @Override public void receiveElectionMessage(int originId, int candidateId,
                int candidateJac, long messageId) {}
        @Override public void receiveEchoMessage(long messageId, int fromWorkerId,
                int bestCandidateId, int bestCandidateJac) {}
        @Override public void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) {}
        @Override public void receiveCoordinatorMessage(int coordinatorId, long messageId) {}
        @Override public Set<Integer> getNeighbourIds() { return neighbours; }
        @Override public void notifyLeaving(int leaverId, Set<Integer> leaverNeighbours) {}
        @Override public List<WorkerEvent> getRecentEvents(long afterSeq) {
            return script.stream().filter(e -> e.seq() > afterSeq).toList();
        }
        @Override public void assignTask(int jobId, String jobType, Object payload) {}
        @Override public void reportTaskResult(int jobId, int workerId, Object result) {}
        @Override public boolean isActive() { return true; }
    }

    private BootstrapRegistryImpl bootstrap;
    private final Map<Integer, WorkerRemote> stubs = new HashMap<>();
    private final ClusterMonitor.WorkerLookup lookup = (id, address) -> stubs.get(id);

    @BeforeEach
    void setUp() throws Exception {
        bootstrap = new BootstrapRegistryImpl();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (WorkerRemote stub : stubs.values()) {
            try {
                UnicastRemoteObject.unexportObject(stub, true);
            } catch (Exception ignored) {
                // best effort
            }
        }
        stubs.clear();
        try {
            UnicastRemoteObject.unexportObject(bootstrap, true);
        } catch (Exception ignored) {
            // best effort
        }
    }

    // Distinct unreachable ports: peer lookup fails with RemoteException (no prune),
    // so directory entries survive like in WorkerAutoRegistrationTest.
    private void register(int id, int port) throws Exception {
        bootstrap.registerWorker(id, "localhost", port);
    }

    private void stub(int id, int jac, int view, Set<Integer> neighbours) throws Exception {
        stubs.put(id, new ConfigurableWorker(id, jac, view, neighbours));
    }

    private ClusterMonitor.ClusterSnapshot snap(int... ids) {
        List<ClusterMonitor.WorkerRow> rows = new java.util.ArrayList<>();
        for (int id : ids) {
            rows.add(new ClusterMonitor.WorkerRow(id, "localhost", 3500 + id,
                    -1, Set.of(), 0, true));
        }
        return new ClusterMonitor.ClusterSnapshot(List.copyOf(rows),
                ClusterMonitor.consensusOf(rows));
    }

    @Test
    void pollBuildsSortedRowsWithRolesAndConsensus() throws Exception {
        register(1, 48881);
        register(2, 48882);
        register(3, 48883);
        stub(1, 0, 2, Set.of(2));
        stub(2, 5, 2, Set.of(1, 3));
        stub(3, 1, 2, Set.of(2));

        ClusterMonitor.ClusterSnapshot snapshot = ClusterMonitor.poll(bootstrap, lookup);

        assertEquals(3, snapshot.rows().size());
        assertEquals(1, snapshot.rows().get(0).workerId());
        assertEquals(2, snapshot.consensusCoordinator());
        assertEquals("Worker", ClusterMonitor.roleOf(snapshot.rows().get(0)));
        assertEquals("Coordinator", ClusterMonitor.roleOf(snapshot.rows().get(1)));
        assertEquals(Set.of(1, 3), snapshot.rows().get(1).neighbours());
        assertEquals(5, snapshot.rows().get(1).jac());
    }

    @Test
    void pollMarksMissingStubAsUnreachableRow() throws Exception {
        register(1, 48881);
        register(2, 48882);
        stub(1, 0, 1, Set.of());

        ClusterMonitor.ClusterSnapshot snapshot = ClusterMonitor.poll(bootstrap, lookup);

        assertEquals(2, snapshot.rows().size());
        ClusterMonitor.WorkerRow dead = snapshot.rows().get(1);
        assertEquals(2, dead.workerId());
        assertEquals("Unreachable", ClusterMonitor.roleOf(dead));
        // The live worker's vote still decides the consensus.
        assertEquals(1, snapshot.consensusCoordinator());
    }

    @Test
    void pollSurvivesThrowingLookup() throws Exception {
        register(1, 48881);
        stub(1, 0, 1, Set.of());

        ClusterMonitor.WorkerLookup throwing = (id, address) -> {
            throw new RuntimeException("registry down");
        };
        ClusterMonitor.ClusterSnapshot snapshot = ClusterMonitor.poll(bootstrap, throwing);

        assertEquals(1, snapshot.rows().size());
        assertEquals("Unreachable", ClusterMonitor.roleOf(snapshot.rows().get(0)));
        assertEquals(-1, snapshot.consensusCoordinator());
    }

    @Test
    void firstPollEmitsOneSummaryLine() throws Exception {
        register(1, 48881);
        register(2, 48882);
        stub(1, 0, 1, Set.of(2));
        stub(2, 0, 1, Set.of(1));

        ClusterMonitor.ClusterSnapshot curr = ClusterMonitor.poll(bootstrap, lookup);
        List<ClusterMonitor.ClusterEvent> events = ClusterMonitor.diff(null, curr);

        assertEquals(1, events.size());
        assertTrue(events.get(0).text().contains("2 workers"));
        assertTrue(events.get(0).text().contains("Worker 1"));
    }

    @Test
    void identicalSnapshotsEmitNothing() {
        ClusterMonitor.ClusterSnapshot snapshot = snap(1, 2);

        assertTrue(ClusterMonitor.diff(snapshot, snap(1, 2)).isEmpty());
    }

    @Test
    void joinAndLeaveAreReported() {
        List<ClusterMonitor.ClusterEvent> events =
                ClusterMonitor.diff(snap(1, 2), snap(2, 3));

        List<String> texts = events.stream().map(ClusterMonitor.ClusterEvent::text).toList();
        assertEquals(2, texts.size());
        assertTrue(texts.get(0).contains("Worker 3 joined"));
        assertTrue(texts.get(0).contains("3503"));
        assertTrue(texts.get(1).contains("Worker 1 left"));
    }

    @Test
    void electionChangesAreReported() throws Exception {
        register(1, 48881);
        stub(1, 0, 1, Set.of());

        ClusterMonitor.ClusterSnapshot before = ClusterMonitor.poll(bootstrap, lookup);
        ((ConfigurableWorker) stubs.get(1)).view = 2;
        // A second worker reporting view 2 makes it the consensus.
        register(2, 48882);
        stub(2, 0, 2, Set.of());
        ClusterMonitor.ClusterSnapshot after = ClusterMonitor.poll(bootstrap, lookup);

        // before consensus is 1, after is 2.
        assertEquals(1, before.consensusCoordinator());
        assertEquals(2, after.consensusCoordinator());
        List<String> texts = ClusterMonitor.diff(before, after).stream()
                .map(ClusterMonitor.ClusterEvent::text).toList();
        assertTrue(texts.stream().anyMatch(t -> t.contains("New coordinator elected: Worker 2")),
                "expected election line, got: " + texts);
    }

    @Test
    void coordinatorLossIsReported() {
        ClusterMonitor.ClusterSnapshot before = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(1, "localhost", 3501, 1, Set.of(), 0, true)), 1);
        ClusterMonitor.ClusterSnapshot after = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(1, "localhost", 3501, -1, Set.of(), 0, true)), -1);

        List<String> texts = ClusterMonitor.diff(before, after).stream()
                .map(ClusterMonitor.ClusterEvent::text).toList();
        assertTrue(texts.stream().anyMatch(t -> t.contains("Coordinator lost")),
                "expected loss line, got: " + texts);
    }

    @Test
    void neighbourChangeIsReportedAsRewire() {
        ClusterMonitor.ClusterSnapshot before = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(1, "localhost", 3501, 2, Set.of(2), 0, true)), 2);
        ClusterMonitor.ClusterSnapshot after = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(1, "localhost", 3501, 2, Set.of(3), 0, true)), 2);

        List<String> texts = ClusterMonitor.diff(before, after).stream()
                .map(ClusterMonitor.ClusterEvent::text).toList();
        assertEquals(1, texts.size());
        assertTrue(texts.get(0).contains("Worker 1 rewired"));
        assertTrue(texts.get(0).contains("[2] -> [3]"));
    }

    @Test
    void unreachableTransitionBeatsRewireNoise() {
        ClusterMonitor.ClusterSnapshot before = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(1, "localhost", 3501, 2, Set.of(2), 0, true)), 2);
        ClusterMonitor.ClusterSnapshot after = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(1, "localhost", 3501, -1, Set.of(), 0, false)), -1);

        List<String> texts = ClusterMonitor.diff(before, after).stream()
                .map(ClusterMonitor.ClusterEvent::text).toList();
        assertTrue(texts.stream().anyMatch(t -> t.contains("unreachable")),
                "expected unreachable line, got: " + texts);
        assertTrue(texts.stream().noneMatch(t -> t.contains("rewired")),
                "must not double-report a rewire for a dead worker, got: " + texts);
    }

    @Test
    void jacIncreaseIsReportedAsAssignment() {
        ClusterMonitor.ClusterSnapshot before = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(2, "localhost", 3502, 2, Set.of(1), 5, true)), 2);
        ClusterMonitor.ClusterSnapshot after = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(2, "localhost", 3502, 2, Set.of(1), 8, true)), 2);

        List<String> texts = ClusterMonitor.diff(before, after).stream()
                .map(ClusterMonitor.ClusterEvent::text).toList();
        assertEquals(1, texts.size());
        assertTrue(texts.get(0).contains("assigned 3 tasks"));
        assertTrue(texts.get(0).contains("JAC 5 -> 8"));
    }

    @Test
    void consensusTieBreaksToSmallestId() {
        ClusterMonitor.WorkerRow a =
                new ClusterMonitor.WorkerRow(1, "localhost", 3501, 3, Set.of(), 0, true);
        ClusterMonitor.WorkerRow b =
                new ClusterMonitor.WorkerRow(2, "localhost", 3502, 2, Set.of(), 0, true);

        assertEquals(2, ClusterMonitor.consensusOf(List.of(a, b)));
    }

    @Test
    void firstPollSeedsSequencesWithoutEmitting() throws Exception {
        register(1, 48881);
        ConfigurableWorker w1 = new ConfigurableWorker(1, 0, 1, Set.of());
        w1.script = List.of(new WorkerEvent(0, 1, 1000L,
                WorkerEvent.ELECTION_STARTED, "started election wave 7 (JAC 0)"));
        stubs.put(1, w1);

        Map<Integer, Long> lastSeq = new HashMap<>();
        ClusterMonitor.PollResult result = ClusterMonitor.pollWithEvents(bootstrap, lookup, lastSeq);

        assertTrue(result.workerEvents().isEmpty(), "backlog seeds the cursor, not the feed");
        assertEquals(0L, lastSeq.get(1));
        assertEquals(1, result.snapshot().rows().size());
    }

    @Test
    void secondPollEmitsOnlyUnseenEvents() throws Exception {
        register(1, 48881);
        ConfigurableWorker w1 = new ConfigurableWorker(1, 0, 1, Set.of());
        stubs.put(1, w1);

        Map<Integer, Long> lastSeq = new HashMap<>();
        ClusterMonitor.pollWithEvents(bootstrap, lookup, lastSeq);
        w1.script = List.of(
                new WorkerEvent(0, 1, 1000L, WorkerEvent.ELECTION_STARTED, "started election wave 7"),
                new WorkerEvent(1, 1, 1001L, WorkerEvent.COORDINATOR_ANNOUNCED, "announced coordinator Worker 1"));
        ClusterMonitor.PollResult result = ClusterMonitor.pollWithEvents(bootstrap, lookup, lastSeq);

        assertEquals(2, result.workerEvents().size());
        assertTrue(result.workerEvents().get(0).text().contains("started election wave 7"));
        // Draining twice replays nothing.
        assertTrue(ClusterMonitor.pollWithEvents(bootstrap, lookup, lastSeq)
                .workerEvents().isEmpty());
    }

    @Test
    void exactRewireEventsSuppressTheHeuristicLine() {
        ClusterMonitor.ClusterSnapshot before = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(1, "localhost", 3501, 2, Set.of(2), 0, true)), 2);
        ClusterMonitor.ClusterSnapshot after = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(1, "localhost", 3501, 2, Set.of(3), 0, true)), 2);
        Map<Integer, Set<String>> exact =
                Map.of(1, Set.of(WorkerEvent.NEIGHBOUR_REMOVED, WorkerEvent.NEIGHBOUR_ADDED));

        List<String> texts = ClusterMonitor.diff(before, after, exact).stream()
                .map(ClusterMonitor.ClusterEvent::text).toList();
        assertTrue(texts.isEmpty(), "exact lines already tell it, got: " + texts);
    }

    @Test
    void exactDispatchSuppressesTheJacLine() {
        ClusterMonitor.ClusterSnapshot before = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(2, "localhost", 3502, 2, Set.of(1), 5, true)), 2);
        ClusterMonitor.ClusterSnapshot after = new ClusterMonitor.ClusterSnapshot(
                List.of(new ClusterMonitor.WorkerRow(2, "localhost", 3502, 2, Set.of(1), 8, true)), 2);
        Map<Integer, Set<String>> exact = Map.of(2, Set.of(WorkerEvent.TASK_DISPATCHED));

        assertTrue(ClusterMonitor.diff(before, after, exact).isEmpty());
    }
}
