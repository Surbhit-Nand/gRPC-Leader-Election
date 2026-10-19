package com.cs324.client;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerEvent;
import com.cs324.common.WorkerRemote;

import java.rmi.RemoteException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// Headless cluster observer for the GUI: polls the directory plus every worker
// (snapshot rows and each worker's sequenced event log), then merges exact
// worker happenings with snapshot-diff lines for what workers cannot self-report.
// All network calls happen on the caller's thread, never on the Swing event thread.
public final class ClusterMonitor {

    // Resolves a worker stub for one directory entry; null means unreachable.
    // Production passes a gRPC adapter; tests may pass a map-backed fake.
    public interface WorkerLookup {
        WorkerRemote lookup(int workerId, WorkerAddress address);
    }

    // One polled worker. Unreachable rows keep id/host/port so the table
    // can still show them; their view is -1 and their JAC reads as 0.
    public record WorkerRow(int workerId, String rmiHost, int rmiPort,
            int coordinatorView, Set<Integer> neighbours, int jac, boolean alive) {}

    // Consensus coordinator across alive rows: most-reported non--1 view,
    // smallest id wins ties; -1 when nobody knows one yet.
    public record ClusterSnapshot(List<WorkerRow> rows, int consensusCoordinator) {}

    // One feed line with its timestamp; the UI formats the time.
    public record ClusterEvent(Instant time, String text) {}

    private ClusterMonitor() {}

    // Snapshot the whole cluster. A dead worker becomes an unreachable row;
    // only a dead directory fails the poll.
    public static ClusterSnapshot poll(BootstrapRemote bootstrap, WorkerLookup lookup)
            throws RemoteException {
        return pollWithEvents(bootstrap, lookup, new HashMap<>()).snapshot();
    }

    // Snapshot plus this round's exact worker events, oldest first. lastSeq tracks
    // the newest sequence shown per worker across polls; unknown workers are
    // seeded silently (their backlog becomes the baseline, not feed spam).
    // Returned alongside the snapshot so the caller paints both atomically.
    public record PollResult(ClusterSnapshot snapshot, List<ClusterEvent> workerEvents,
            Map<Integer, Set<String>> exactKindsByWorker) {}

    public static PollResult pollWithEvents(BootstrapRemote bootstrap, WorkerLookup lookup,
            Map<Integer, Long> lastSeq) throws RemoteException {
        List<Integer> ids = bootstrap.getActiveWorkerIds();
        List<WorkerRow> rows = new ArrayList<>();
        List<ClusterEvent> workerEvents = new ArrayList<>();
        Map<Integer, Set<String>> exactKinds = new LinkedHashMap<>();
        if (ids != null) {
            List<Integer> sorted = new ArrayList<>(ids);
            Collections.sort(sorted);
            for (int id : sorted) {
                WorkerAddress address;
                try {
                    address = bootstrap.getWorkerAddress(id);
                } catch (Exception e) {
                    continue;
                }
                if (address == null) {
                    continue;
                }
                WorkerRemote stub;
                try {
                    stub = lookup.lookup(id, address);
                } catch (Exception e) {
                    stub = null;
                }
                WorkerRow row = readRow(id, address, stub);
                rows.add(row);
                drainFromStub(row, stub, lastSeq, workerEvents, exactKinds);
            }
        }
        ClusterSnapshot snapshot = new ClusterSnapshot(List.copyOf(rows), consensusOf(rows));
        workerEvents.sort((a, b) -> a.time().compareTo(b.time()));
        return new PollResult(snapshot, List.copyOf(workerEvents), exactKinds);
    }

    // Fetches one worker's unseen events; first sighting only advances the cursor.
    private static void drainFromStub(WorkerRow row, WorkerRemote stub,
            Map<Integer, Long> lastSeq, List<ClusterEvent> out, Map<Integer, Set<String>> exactKinds) {
        if (!row.alive() || stub == null) {
            return;
        }
        boolean seeded = lastSeq.containsKey(row.workerId());
        long afterSeq = lastSeq.getOrDefault(row.workerId(), -1L);
        List<WorkerEvent> fresh;
        try {
            fresh = stub.getRecentEvents(afterSeq);
        } catch (Exception e) {
            return;
        }
        if (fresh == null || fresh.isEmpty()) {
            lastSeq.putIfAbsent(row.workerId(), afterSeq);
            return;
        }
        long max = afterSeq;
        for (WorkerEvent event : fresh) {
            max = Math.max(max, event.seq());
        }
        lastSeq.put(row.workerId(), max);
        if (!seeded) {
            return;
        }
        Set<String> kinds = exactKinds.computeIfAbsent(row.workerId(), k -> new HashSet<>());
        for (WorkerEvent event : fresh) {
            kinds.add(event.kind());
            out.add(new ClusterEvent(Instant.ofEpochMilli(event.epochMillis()),
                    "Worker " + event.workerId() + ": " + event.detail()));
        }
    }

    // Three successful reads prove liveness, so no extra ping call per worker.
    private static WorkerRow readRow(int id, WorkerAddress address, WorkerRemote stub) {
        if (stub == null) {
            return unreachable(id, address);
        }
        try {
            int view = stub.getCurrentCoordinatorId();
            int jac = stub.getJobAllocationCounter();
            Set<Integer> neighbours = stub.getNeighbourIds();
            return new WorkerRow(id, address.getRmiHost(), address.getRmiPort(),
                    view, neighbours != null ? Set.copyOf(neighbours) : Set.of(),
                    jac, true);
        } catch (Exception e) {
            return unreachable(id, address);
        }
    }

    private static WorkerRow unreachable(int id, WorkerAddress address) {
        return new WorkerRow(id, address.getRmiHost(), address.getRmiPort(),
                -1, Set.of(), 0, false);
    }

    static int consensusOf(List<WorkerRow> rows) {
        Map<Integer, Integer> votes = new TreeMap<>();
        for (WorkerRow row : rows) {
            if (row.alive() && row.coordinatorView() != -1) {
                votes.merge(row.coordinatorView(), 1, Integer::sum);
            }
        }
        int best = -1;
        int bestVotes = 0;
        for (Map.Entry<Integer, Integer> vote : votes.entrySet()) {
            if (vote.getValue() > bestVotes) {
                best = vote.getKey();
                bestVotes = vote.getValue();
            }
        }
        return best;
    }

    // Display role for one table row.
    public static String roleOf(WorkerRow row) {
        if (!row.alive()) {
            return "Unreachable";
        }
        if (row.coordinatorView() != -1 && row.coordinatorView() == row.workerId()) {
            return "Coordinator";
        }
        return "Worker";
    }

    // Derives feed events from two snapshots. A null prev means first poll:
    // one summary line instead of a join per worker.
    public static List<ClusterEvent> diff(ClusterSnapshot prev, ClusterSnapshot curr) {
        return diff(prev, curr, Map.of());
    }

    // Same, but skips a heuristic line when the worker's own exact events already
    // tell that story this round (rewire heuristic vs NEIGHBOUR_ADDED/REMOVED,
    // JAC heuristic vs TASK_DISPATCHED). Death/reachability lines always stay:
    // a dead worker cannot report itself.
    public static List<ClusterEvent> diff(ClusterSnapshot prev, ClusterSnapshot curr,
            Map<Integer, Set<String>> exactKindsByWorker) {
        List<ClusterEvent> events = new ArrayList<>();
        Instant now = Instant.now();
        if (prev == null) {
            events.add(new ClusterEvent(now, summaryLine(curr)));
            return events;
        }
        Map<Integer, WorkerRow> before = byId(prev.rows());
        Map<Integer, WorkerRow> after = byId(curr.rows());

        for (WorkerRow row : curr.rows()) {
            if (!before.containsKey(row.workerId())) {
                events.add(new ClusterEvent(now, "Worker " + row.workerId()
                        + " joined (port " + row.rmiPort() + ")"));
            }
        }
        for (WorkerRow row : prev.rows()) {
            if (!after.containsKey(row.workerId())) {
                events.add(new ClusterEvent(now, "Worker " + row.workerId() + " left"));
            }
        }
        for (WorkerRow row : curr.rows()) {
            WorkerRow old = before.get(row.workerId());
            if (old == null) {
                continue;
            }
            if (old.alive() && !row.alive()) {
                events.add(new ClusterEvent(now, "Worker " + row.workerId() + " unreachable"));
            } else if (!old.alive() && row.alive()) {
                events.add(new ClusterEvent(now, "Worker " + row.workerId() + " reachable again"));
            } else if (row.alive() && old.alive()
                    && !row.neighbours().equals(old.neighbours())
                    && !hasExact(exactKindsByWorker, row.workerId(),
                            WorkerEvent.NEIGHBOUR_ADDED, WorkerEvent.NEIGHBOUR_REMOVED)) {
                events.add(new ClusterEvent(now, "Worker " + row.workerId() + " rewired: neighbours "
                        + formatIds(old.neighbours()) + " -> " + formatIds(row.neighbours())));
            }
            if (row.alive() && old.alive() && row.jac() > old.jac()
                    && !hasExact(exactKindsByWorker, row.workerId(), WorkerEvent.TASK_DISPATCHED)) {
                int delta = row.jac() - old.jac();
                events.add(new ClusterEvent(now, "Worker " + row.workerId() + " assigned "
                        + delta + (delta == 1 ? " task" : " tasks")
                        + " (JAC " + old.jac() + " -> " + row.jac() + ")"));
            }
        }
        if (prev.consensusCoordinator() != curr.consensusCoordinator()) {
            events.add(new ClusterEvent(now, electionLine(prev.consensusCoordinator(),
                    curr.consensusCoordinator())));
        }
        return events;
    }

    private static boolean hasExact(Map<Integer, Set<String>> exactKindsByWorker, int workerId,
            String... kinds) {
        Set<String> kindsSeen = exactKindsByWorker.get(workerId);
        if (kindsSeen == null) {
            return false;
        }
        for (String kind : kinds) {
            if (kindsSeen.contains(kind)) {
                return true;
            }
        }
        return false;
    }

    private static String summaryLine(ClusterSnapshot curr) {        int n = curr.rows().size();
        String workers = n == 1 ? "1 worker" : n + " workers";
        if (curr.consensusCoordinator() == -1) {
            return "Cluster view loaded: " + workers + ", no coordinator yet";
        }
        return "Cluster view loaded: " + workers
                + ", coordinator Worker " + curr.consensusCoordinator();
    }

    private static String electionLine(int before, int after) {
        if (after == -1) {
            return "Coordinator lost (was Worker " + before + ")";
        }
        if (before == -1) {
            return "Coordinator elected: Worker " + after;
        }
        return "New coordinator elected: Worker " + after + " (was Worker " + before + ")";
    }

    private static Map<Integer, WorkerRow> byId(List<WorkerRow> rows) {
        Map<Integer, WorkerRow> out = new LinkedHashMap<>();
        for (WorkerRow row : rows) {
            out.put(row.workerId(), row);
        }
        return out;
    }

    private static String formatIds(Set<Integer> ids) {
        List<Integer> sorted = new ArrayList<>(ids);
        Collections.sort(sorted);
        return sorted.toString();
    }
}
