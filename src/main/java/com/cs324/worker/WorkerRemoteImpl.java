package com.cs324.worker;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.TaskAssignment;
import com.cs324.common.ClientRemote;
import com.cs324.common.Constants;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerEvent;
import com.cs324.common.WorkerRemote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.NotBoundException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerRemoteImpl extends UnicastRemoteObject implements WorkerRemote {

    // One election wave, plus the bookkeeping needed to complete it.
    private static class ElectionState {
        // Neighbour that forwarded this wave, so results can be echoed back to it.
        int parentId;
        // Best candidate in this wave so far, compared by JAC then by id.
        int bestCandidateId;
        int bestCandidateJac;
        // Neighbours that have not answered yet; the wave completes when this empties.
        final Set<Integer> pendingChildren = ConcurrentHashMap.newKeySet();
        // Latch so a wave echoes upward exactly once, however many triggers fire.
        boolean echoed = false;

        ElectionState(int parentId, int bestCandidateId, int bestCandidateJac) {
            this.parentId = parentId;
            this.bestCandidateId = bestCandidateId;
            this.bestCandidateJac = bestCandidateJac;
        }
    }


    private final int id;
    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private final AtomicLong electionCounter = new AtomicLong(0);
    private volatile int currentCoordinatorId = -1;
    private final Set<Long> seenMessageIds = ConcurrentHashMap.newKeySet();
    private final Set<Long> seenCoordinatorMessageIds = ConcurrentHashMap.newKeySet();
    private final Map<Long, ElectionState> elections = new ConcurrentHashMap<>();
    private final Map<Integer, WorkerRemote> neighbours = new ConcurrentHashMap<>();
    // Last-known neighbour set per direct neighbour (2-hop view for grandparent rewire).
    private final Map<Integer, Set<Integer>> twoHop = new ConcurrentHashMap<>();
    // Once-guard so concurrent failures on the same dead id rewire exactly once.
    private final Set<Integer> repairInProgress = ConcurrentHashMap.newKeySet();
    // Recent happenings for the GUI feed; a bounded ring so a chatty cluster cannot leak memory.
    private final Deque<WorkerEvent> recentEvents = new ArrayDeque<>();
    private static final int MAX_RECENT_EVENTS = 64;
    private final AtomicLong eventSeq = new AtomicLong(0);

    // Composite key, so two workers' results for one job cannot overwrite each other.
    private record JobResultKey(int jobId, int workerId) implements java.io.Serializable {}

    // Pool for job slices; daemon threads, so a missed shutdown cannot hang the JVM.
    private final ExecutorService executor = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
            new ThreadFactory() {
                private final AtomicInteger seq = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "worker-task-" + seq.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            });
    // Partial results, keyed by job and worker until the job is aggregated.
    private final Map<JobResultKey, Object> taskResults = new ConcurrentHashMap<>();
    // Exactly-once guard, claimed before submit so duplicate assignTask calls collapse.
    private final Set<Integer> seenJobIds = ConcurrentHashMap.newKeySet();
    // Observability only - never a lock, so nothing waits on it.
    private final Set<Integer> activeTasks = ConcurrentHashMap.newKeySet();

    // One in-flight client job: who owes us a result, and where to deliver it.
    private record JobState(String jobType, Set<Integer> expected, String clientHost, int clientPort, String clientName, AtomicBoolean dispatchComplete) {}

    // Stops the same job id being coordinated twice.
    private final Set<Integer> submittedJobIds = ConcurrentHashMap.newKeySet();

    // Jobs this worker is coordinating right now.
    private final Map<Integer, JobState> coordinatingJobs = new ConcurrentHashMap<>();

    // Single daemon scheduler for coordinator liveness probes.
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "worker-heartbeat");
        t.setDaemon(true);
        return t;
    });
    // Bootstrap directory, set once on join; null in tests so we fall back to peers.
    private volatile BootstrapRemote bootstrap;
    // Local host as published to peers; set once on join for repair reverse edges.
    private volatile String localHost;
    // Cached coordinator stub, valid only while cachedCoordinatorId still matches.
    private volatile WorkerRemote coordinatorStub;
    private volatile int cachedCoordinatorId = -1;
    // Keeps a single probe in flight so heartbeat ticks never overlap.
    private final AtomicBoolean heartbeatInFlight = new AtomicBoolean(false);
    // Client jobs served this term, reserved by CAS and never above JOBS_PER_TERM.
    private final AtomicInteger termJobsCoordinated = new AtomicInteger(0);
    // Closed between winning a rotation and the next coordinator announcement.
    private final AtomicBoolean handoverInProgress = new AtomicBoolean(false);

    public WorkerRemoteImpl(int id) throws RemoteException {
        super();
        this.id = id;
    }

    @Override
    public int getId() throws RemoteException {
        return id;
    }

    @Override
    public int getJobAllocationCounter() throws RemoteException {
        return jobAllocationCounter.get();
    }

    @Override
    public int getCurrentCoordinatorId() throws RemoteException {
        return currentCoordinatorId;
    }

    @Override
    public Set<Integer> getNeighbourIds() throws RemoteException {
        return Set.copyOf(neighbours.keySet());
    }

    // Appends one happening to the ring, dropping the oldest past the cap.
    private void recordEvent(String kind, String detail) {
        WorkerEvent event = new WorkerEvent(eventSeq.getAndIncrement(), id,
                System.currentTimeMillis(), kind, detail);
        synchronized (recentEvents) {
            recentEvents.addLast(event);
            while (recentEvents.size() > MAX_RECENT_EVENTS) {
                recentEvents.removeFirst();
            }
        }
    }

    @Override
    public List<WorkerEvent> getRecentEvents(long afterSeq) throws RemoteException {
        synchronized (recentEvents) {
            List<WorkerEvent> out = new ArrayList<>();
            for (WorkerEvent event : recentEvents) {
                if (event.seq() > afterSeq) {
                    out.add(event);
                }
            }
            return out;
        }
    }

    // Clean-exit handoff: hands our neighbour set to every neighbour so survivors
    // chain to a grandparent immediately. Best effort; crashes skip this entirely
    // and rely on survivor-side failure detection instead.
    public int notifyNeighboursOfLeaving() {
        Set<Integer> mine = Set.copyOf(neighbours.keySet());
        int notified = 0;
        for (WorkerRemote peer : new ArrayList<>(neighbours.values())) {
            try {
                peer.notifyLeaving(id, mine);
                notified++;
            } catch (Exception ignored) {
                // best effort on shutdown
            }
        }
        recordEvent(WorkerEvent.LEAVING,
                "leaving; handed neighbours " + sortedIds(mine) + " to " + notified + " neighbour(s)");
        return notified;
    }

    private static List<Integer> sortedIds(Set<Integer> ids) {
        List<Integer> sorted = new ArrayList<>(ids);
        Collections.sort(sorted);
        return sorted;
    }

    public int getJobsCoordinatedInTerm() {
        return termJobsCoordinated.get();
    }

    @Override
    public boolean isActive() throws RemoteException {
        return true;
    }

    
    // Starts a new wave: we are the origin and begin as our own best candidate.
    @Override
    public long startElection() {
        // Worker id in the high bits keeps message ids globally unique without a registry.
        long messageId = ((long) id << 32) | (electionCounter.getAndIncrement());
        seenMessageIds.add(messageId);

        int currentJac = jobAllocationCounter.get();
        ElectionState election = new ElectionState(this.id, this.id, currentJac);
        recordEvent(WorkerEvent.ELECTION_STARTED,
                "started election wave " + messageId + " (JAC " + currentJac + ")");

        for (Integer neighborId : neighbours.keySet()) {
            election.pendingChildren.add(neighborId);
        }

        elections.put(messageId, election);

        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            try {
                entry.getValue().receiveElectionMessage(this.id, this.id, currentJac, messageId);
            } catch (RemoteException e) {
                election.pendingChildren.remove(entry.getKey());
                handleDeadNeighbour(entry.getKey());
            }
        }

        // No reachable neighbour, so this wave is already complete and echoes immediately.
        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
        return messageId;
    }

    @Override
    // Folds an incoming wave in: we win on lower JAC, then on higher id.
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException {
        seenMessageIds.add(messageId);

        int currentJac = jobAllocationCounter.get();
        int bestId = candidateId;
        int bestJac = candidateJac;

        if (currentJac < bestJac) {
            bestId = this.id;
            bestJac = currentJac;
        } else if (currentJac == bestJac) {
            if (this.id > bestId) {
                bestId = this.id;
                bestJac = currentJac;
            }
        }

        final int ourBestId = bestId;
        final int ourBestJac = bestJac;
        final int parentId = originId;

        final ElectionState[] created = new ElectionState[1];
        final ElectionState[] alreadySeen = new ElectionState[1];
        // Claim and publish in one atomic step, so a duplicate never sees the id without state.
        elections.compute(messageId, (key, existing) -> {
            if (existing != null) {
                alreadySeen[0] = existing;
                return existing;
            }
            ElectionState election = new ElectionState(parentId, ourBestId, ourBestJac);
            for (Integer neighborId : neighbours.keySet()) {
                if (neighborId != parentId) {
                    election.pendingChildren.add(neighborId);
                }
            }
            created[0] = election;
            return election;
        });

        // Already seen this wave: answer the sender so it can finish, but do not forward again.
        ElectionState seen = alreadySeen[0];
        if (seen != null) {
            int[] best = new int[2];
                // Read the best candidate under the wave's lock so a concurrent echo cannot tear it.
            synchronized (seen) {
                best[0] = seen.bestCandidateId;
                best[1] = seen.bestCandidateJac;
            }
            try {
                WorkerRemote sender = neighbours.get(parentId);
                if (sender != null) {
                    sender.receiveEchoMessage(messageId, this.id, best[0], best[1]);
                }
            } catch (RemoteException ignored) {
                handleDeadNeighbour(parentId);
            }
            return;
        }

        ElectionState election = created[0];
        // Lazy 2-hop seed: first wave from a new parent teaches us its neighbour set.
        if (!twoHop.containsKey(parentId)) {
            WorkerRemote parentStub = neighbours.get(parentId);
            if (parentStub != null) {
                refreshTwoHop(parentId, parentStub);
            }
        }

        // Forward to every neighbour except the one this wave arrived from.
        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            if (entry.getKey() != originId) {
                try {
                    entry.getValue().receiveElectionMessage(this.id, ourBestId, ourBestJac, messageId);
                } catch (RemoteException e) {
                    // Unreachable neighbour: stop waiting on it and rewire past it.
                    election.pendingChildren.remove(entry.getKey());
                    handleDeadNeighbour(entry.getKey());
                }
            }
        }

        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
    }

    @Override
    // Applies a coordinator announcement and relays it onward to the whole network.
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        // First sighting; a broadcast that loops back to us stops here.
        if (!seenCoordinatorMessageIds.add(messageId)) {
            return;
        }
        this.currentCoordinatorId = coordinatorId;
        handoverInProgress.set(false);
        if (coordinatorId == this.id) {  
           termJobsCoordinated.set(0);
        }

        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            try {
                entry.getValue().receiveCoordinatorMessage(coordinatorId, messageId);
            } catch (RemoteException ignored) {
                handleDeadNeighbour(entry.getKey());
            }
        }
    }

    // Local only: a joiner learned this from one peer, and relaying would read as a new term.
    public void adoptCoordinator(int coordinatorId, long messageId) {
        seenCoordinatorMessageIds.add(messageId);
        this.currentCoordinatorId = coordinatorId;
        recordEvent(WorkerEvent.COORDINATOR_ADOPTED, "adopted coordinator Worker " + coordinatorId);
        if (coordinatorId == this.id) {
            termJobsCoordinated.set(0);
        }
    }

    @Override
    // Folds a child's answer in and completes the wave once nobody is outstanding.
    public void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException {
        ElectionState election = elections.get(messageId);
        if (election == null) {
            return;
        }

        // Merging and the completeness test must be one critical section, or echoes race.
        synchronized (election) {
            if (bestCandidateJac < election.bestCandidateJac) {
                election.bestCandidateId = bestCandidateId;
                election.bestCandidateJac = bestCandidateJac;
            } else if (election.bestCandidateJac == bestCandidateJac) {
                if (bestCandidateId > election.bestCandidateId) {
                    election.bestCandidateId = bestCandidateId;
                    election.bestCandidateJac = bestCandidateJac;
                }
            }

            election.pendingChildren.remove(fromWorkerId);

                // Last outstanding child: complete the wave. The echoed latch keeps it once-only.
            if (election.pendingChildren.isEmpty() && !election.echoed) {
                propagateEcho(messageId, election);
            }
        }
    }

    // The root announces the winner; every other node relays the result upward.
    private void propagateEcho(long messageId, ElectionState election) {
        synchronized (election) {
            // Set under the lock, so concurrent triggers cannot announce twice.
            if (election.echoed) {
                return;
            }
            election.echoed = true;
        }

        // We started this wave, so we are the root and decide the outcome.
        if (election.parentId == this.id) {
            this.currentCoordinatorId = election.bestCandidateId;
            handoverInProgress.set(false);
            if (election.bestCandidateId == this.id) {
                termJobsCoordinated.set(0);
            }
            recordEvent(WorkerEvent.COORDINATOR_ANNOUNCED, "announced coordinator Worker "
                    + election.bestCandidateId + " (wave " + messageId + ")");
            // Offset, so a coordinator announcement can never collide with an election id.
            long coordMessageId = messageId + 1000000L;
            // Record before broadcasting, or our own announcement returns and resets the term.
            seenCoordinatorMessageIds.add(coordMessageId);
            List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
            for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
                try {
                    entry.getValue().receiveCoordinatorMessage(election.bestCandidateId, coordMessageId);
                } catch (RemoteException ignored) {
                    handleDeadNeighbour(entry.getKey());
                }
            }
        } else {
            // Send our merged best back to the node that forwarded this wave.
            WorkerRemote parentStub = neighbours.get(election.parentId);
            if (parentStub != null) {
                try {
                    parentStub.receiveEchoMessage(messageId, this.id, election.bestCandidateId, election.bestCandidateJac);
                } catch (RemoteException ignored) {
                    handleDeadNeighbour(election.parentId);
                }
            }
        }
    }

    @Override
    // Adds a peer by looking up its registry stub; also used for the reverse edge.
    public void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException {
        try {
            Registry registry = LocateRegistry.getRegistry(rmiHost, rmiPort);
            WorkerRemote stub = (WorkerRemote) registry.lookup("Worker" + newWorker);
            neighbours.put(newWorker, stub);
            // Seed the 2-hop cache while the peer is alive; best effort, never fails the connect.
            refreshTwoHop(newWorker, stub);
            recordEvent(WorkerEvent.NEIGHBOUR_ADDED, "linked Worker " + newWorker);
        } catch (NotBoundException e) {
            throw new RemoteException("Failed to look up new neighbour", e);
        }
    }

    @Override
    // Graceful-leave fast path: chain to a grandparent now instead of waiting for a probe to fail.
    public void notifyLeaving(int leaverId, Set<Integer> leaverNeighbours) throws RemoteException {
        if (leaverNeighbours != null) {
            twoHop.put(leaverId, Set.copyOf(leaverNeighbours));
        }
        handleDeadNeighbour(leaverId, "leave");
    }

    // Best-effort fetch of a neighbour's neighbour set; a failure means the peer just died.
    private void refreshTwoHop(int neighbourId, WorkerRemote stub) {
        try {
            Set<Integer> ids = stub.getNeighbourIds();
            if (ids != null) {
                twoHop.put(neighbourId, Set.copyOf(ids));
            }
        } catch (Exception ignored) {
            handleDeadNeighbour(neighbourId);
        }
    }

    // Survivor-side repair: prune the dead edge, then chain to one grandparent (smallest id first).
    // Falls back to any live Bootstrap peer when no grandparent is known or reachable.
    void handleDeadNeighbour(int deadId) {
        handleDeadNeighbour(deadId, "unreachable");
    }

    void handleDeadNeighbour(int deadId, String cause) {
        if (deadId == id || !repairInProgress.add(deadId)) {
            return;
        }
        try {
            neighbours.remove(deadId);
            recordEvent(WorkerEvent.NEIGHBOUR_REMOVED,
                    "dropped Worker " + deadId + " (" + cause + ")");
            Set<Integer> known = twoHop.remove(deadId);
            List<Integer> candidates = new ArrayList<>();
            if (known != null) {
                for (int candidate : known) {
                    if (isRepairCandidate(candidate, deadId)) {
                        candidates.add(candidate);
                    }
                }
                Collections.sort(candidates);
            }
            for (int candidate : candidates) {
                if (tryRepairEdge(candidate, deadId)) {
                    return;
                }
            }
            // No grandparent worked: ask the directory for any other live worker.
            if (bootstrap != null) {
                try {
                    List<Integer> active = new ArrayList<>(bootstrap.getActiveWorkerIds());
                    Collections.sort(active);
                    for (int candidate : active) {
                        if (isRepairCandidate(candidate, deadId) && tryRepairEdge(candidate, deadId)) {
                            return;
                        }
                    }
                } catch (Exception ignored) {
                    // directory unreachable: keep the pruned state, a later wave retries
                }
            }
        } finally {
            repairInProgress.remove(deadId);
        }
    }

    // Eligible rewire target: not us, not the corpse, and not already a neighbour.
    private boolean isRepairCandidate(int candidate, int deadId) {
        return candidate != id && candidate != deadId && !neighbours.containsKey(candidate);
    }

    // Resolves one candidate through Bootstrap and links it bidirectionally; false = try next.
    private boolean tryRepairEdge(int candidate, int deadId) {
        if (bootstrap == null) {
            return false;
        }
        try {
            WorkerAddress address = bootstrap.getWorkerAddress(candidate);
            if (address == null) {
                return false;
            }
            Registry registry = LocateRegistry.getRegistry(address.getRmiHost(), address.getRmiPort());
            WorkerRemote stub = (WorkerRemote) registry.lookup("Worker" + candidate);
            neighbours.put(candidate, stub);
            refreshTwoHop(candidate, stub);
            recordEvent(WorkerEvent.NEIGHBOUR_ADDED,
                    "chained to Worker " + candidate + " past Worker " + deadId);
            try {
                stub.connectAsNeighbour(id, localHostOrDefault(), localPortOrDefault(address));
            } catch (Exception ignored) {
                // forward edge is what heals our partition; a failed reverse just leaves it one-way for now
            }
            return true;
        } catch (Exception ignored) {
            neighbours.remove(candidate);
            return false;
        }
    }

    // Local bind details for the reverse repair edge; resolved from Bootstrap when available.
    private String localHostOrDefault() {
        return localHost != null ? localHost : "localhost";
    }

    private int localPortOrDefault(WorkerAddress candidateAddress) {
        try {
            if (bootstrap != null) {
                WorkerAddress mine = bootstrap.getWorkerAddress(id);
                if (mine != null) {
                    return mine.getRmiPort();
                }
            }
        } catch (Exception ignored) {
            // fall through to the candidate port guess
        }
        return candidateAddress.getRmiPort();
    }

    @Override
    // Coordinator-facing half: claim the job, then compute on a pool thread and report back.
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        // Already accepted (in flight or done), so treat this as a retry and ignore.
        if (!seenJobIds.add(jobId)) {
            return;
        }
        recordEvent(WorkerEvent.TASK_RECEIVED, "job " + jobId + " slice received (" + jobType + ")");

        // A TaskAssignment carries the chunk plus the coordinator to report the result to.
        final Object calculationPayload = payload instanceof TaskAssignment assignment
                ? assignment.payload() : payload;
        final WorkerRemote resultReceiver = payload instanceof TaskAssignment assignment
                ? assignment.coordinator() : null;

        final String typeSnapshot = jobType;
        activeTasks.add(jobId);
        try {
            executor.submit(() -> {
                Object computedResult;
                try {
                    computedResult = WorkerTaskCalculator.calculate(typeSnapshot, calculationPayload);
                } catch (Exception e) {
                    computedResult = "Error executing task: " + e.getMessage();
                }

                try {
                    int targetCoordinator = currentCoordinatorId;
                    // Prefer the coordinator that dispatched us; fall back to whoever leads now.
                    if (resultReceiver != null) {
                        resultReceiver.reportTaskResult(jobId, id, computedResult);
                    } else if (targetCoordinator == id || targetCoordinator == -1) {
                        reportTaskResult(jobId, id, computedResult);
                    } else {
                        WorkerRemote coordinatorStub = neighbours.get(targetCoordinator);
                        if (coordinatorStub != null) {
                            coordinatorStub.reportTaskResult(jobId, id, computedResult);
                        } else {
                            reportTaskResult(jobId, id, computedResult);
                        }
                    }
                } catch (RemoteException ignored) {
                } finally {
                    // Always clear the active marker, even when delivery failed.
                    activeTasks.remove(jobId);
                }
            });
        } catch (RejectedExecutionException e) {
            // Pool refused the work, so undo the claim and let the job be retried elsewhere.
            activeTasks.remove(jobId);
            seenJobIds.remove(jobId);
            throw new RemoteException("Worker overloaded, task rejected for job " + jobId, e);
        }
    }

    @Override
    // Accepts a participant's result once; unknown jobs are simply stored.
    public void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException {
        JobState state = coordinatingJobs.get(jobId);
        if (state == null) {
            taskResults.putIfAbsent(new JobResultKey(jobId, workerId), result);
            return;
        }
        // Per-job lock: record the result and retire the expected participant together.
        synchronized (state) {
            if (!state.expected().contains(workerId)) return;
            taskResults.put(new JobResultKey(jobId, workerId), result);
            state.expected().remove(workerId);
        }
        finishJobIfReady(jobId, state);
    }

    // Delivers the aggregate exactly once, then spends the term if it is used up.
    private void finishJobIfReady(int jobId, JobState state) {
        synchronized (state) {
            if (!state.dispatchComplete().get() || !state.expected().isEmpty()
                    // remove(jobId,state) is the once-only gate: only the thread winning it delivers.
                    || !coordinatingJobs.remove(jobId, state)) return;
        }
        Object result = aggregateJob(state.jobType(), jobId);
        deliverToClient(jobId, result, state);
        maybeRotateCoordinator();
    }

    @Override
    // Coordinator entry point: split the job, dispatch every slice, track who must reply.
    public void submitJob(int jobId, String jobType, Object payload, String clientHost, int clientPort, String clientName) throws RemoteException {
        // Only the current coordinator may split work.
        if (currentCoordinatorId != id) {
            throw new RemoteException("Worker " + id + " is not the current coordinator (coordinator=" + currentCoordinatorId + ")");
        }
        // Refuse during handover, so a term never serves beyond its budget.
        if (handoverInProgress.get()) {
            throw new RemoteException("Worker " + id + " is handing over leadership after a spent term; retry against the new coordinator");
        }
        Map<Integer, WorkerRemote> available = discoverJobWorkers();
        List<Integer> participants = new ArrayList<>();
        participants.add(id);
        // Coordinator first, then peers in id order, for a deterministic split.
        available.keySet().stream().filter(workerId -> workerId != id).sorted().forEach(participants::add);
        List<Object> chunks = splitPayload(jobType, payload, participants.size());
        if (!submittedJobIds.add(jobId)) return;

        final boolean termExhausted = reserveTermSlot(jobId);

        Set<Integer> expected = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < participants.size(); i++) {
            // A participant with no chunk never reports back, so it is not expected.
            if (chunks.get(i) != null) expected.add(participants.get(i));
        }
        JobState state = new JobState(jobType, expected, clientHost, clientPort, clientName, new AtomicBoolean());
        coordinatingJobs.put(jobId, state);
        // Peers report through this stub, so results still reach us if leadership moves.
        WorkerRemote callback = (WorkerRemote) UnicastRemoteObject.toStub(this);

        final String typeSnapshot = jobType;
        for (int i = 0; i < participants.size(); i++) {
            Object chunk = chunks.get(i);
            if (chunk == null) {
                continue;
            }
            int targetId = participants.get(i);
            if (targetId == id) {
                final Object selfChunk = chunk;
                recordEvent(WorkerEvent.TASK_DISPATCHED, "job " + jobId + " slice kept (own chunk)");
                // Our own slice runs on the pool too, so the caller's RMI thread is never blocked.
                executor.submit(() -> {
                    Object result;
                    try {
                        result = WorkerTaskCalculator.calculate(typeSnapshot, selfChunk);
                    } catch (Exception e) {
                        result = "Error executing task: " + e.getMessage();
                    }
                    try {
                        reportTaskResult(jobId, id, result);
                    } catch (RemoteException ignored) {
                    }
                });
            } else {
                // JAC counts only work handed to others, which is what the election compares.
                jobAllocationCounter.incrementAndGet();
                WorkerRemote stub = available.get(targetId);
                if (stub == null) {
                    // In the directory but unreachable: compute its slice here rather than stall.
                    recordEvent(WorkerEvent.TASK_DISPATCHED, "job " + jobId + " slice of Worker "
                            + targetId + " computed locally (peer unreachable)");
                    final Object slice = chunk;
                    executor.submit(() -> {
                        Object fallback;
                        try {
                            fallback = WorkerTaskCalculator.calculate(typeSnapshot, slice);
                        } catch (Exception ex) {
                            // Same contract as the assignTask fallback: a slice that cannot be
                            // computed reports its error, or the job never completes.
                            fallback = "Error executing task: " + ex.getMessage();
                        }
                        try {
                            reportTaskResult(jobId, targetId, fallback);
                        } catch (RemoteException ignored) {
                        }
                    });
                    continue;
                }
                try {
                    stub.assignTask(jobId, typeSnapshot, new TaskAssignment(chunk, callback));
                    recordEvent(WorkerEvent.TASK_DISPATCHED,
                            "job " + jobId + " slice -> Worker " + targetId);
                } catch (RemoteException e) {
                    // Drop the dead peer, rewire past it, and cover its slice locally.
                    handleDeadNeighbour(targetId);
                    recordEvent(WorkerEvent.TASK_DISPATCHED, "job " + jobId + " slice of Worker "
                            + targetId + " computed locally (peer unreachable)");
                    final Object slice = chunk;
                    executor.submit(() -> {
                        Object fallback;
                        try {
                            fallback = WorkerTaskCalculator.calculate(typeSnapshot, slice);
                        } catch (Exception ex) {
                            fallback = "Error executing task: " + ex.getMessage();
                        }
                        try {
                            reportTaskResult(jobId, targetId, fallback);
                        } catch (RemoteException ignored) {
                        }
                    });
                }
            }
        }
        // Set only after every slice is dispatched, so we cannot aggregate early.
        state.dispatchComplete().set(true);
        finishJobIfReady(jobId, state);
        // This job spent the budget, so rotate once its result has been delivered.
        if (termExhausted) {
            maybeRotateCoordinator();
        }
    }

    // Reserves one slot in the term; returns true when that job spent the budget.
    private boolean reserveTermSlot(int jobId) throws RemoteException {
        while (true) {
            int used = termJobsCoordinated.get();
            if (used >= Constants.JOBS_PER_TERM) {
                submittedJobIds.remove(jobId);
                throw new RemoteException("Worker " + id + " has coordinated the maximum of "
                        + Constants.JOBS_PER_TERM + " jobs for this term; it is handing over leadership");
            }
            // Increment and check are one CAS, so the count is never observable above the cap.
            if (termJobsCoordinated.compareAndSet(used, used + 1)) {
                return used + 1 == Constants.JOBS_PER_TERM;
            }
        }
    }

    // Asks the directory for every active worker, so work reaches beyond our neighbours.
    private Map<Integer, WorkerRemote> discoverJobWorkers() throws RemoteException {
        Map<Integer, WorkerRemote> workers = new HashMap<>();
        BootstrapRemote directory = bootstrap;
        // No directory (tests): our neighbours are the whole cluster.
        if (directory == null) {
            workers.putAll(neighbours);
        } else {
            for (int workerId : directory.getActiveWorkerIds()) {
                if (workerId == id) continue;
                WorkerAddress address = directory.getWorkerAddress(workerId);
                if (address == null) continue;
                try {
                    WorkerRemote worker = (WorkerRemote) LocateRegistry.getRegistry(
                            address.getRmiHost(), address.getRmiPort()).lookup("Worker" + workerId);
                        // Skip workers the directory still lists but that no longer answer.
                    if (worker.isActive()) workers.put(workerId, worker);
                } catch (RemoteException | NotBoundException ignored) {
                }
            }
        }
        // We always take part in our own job.
        workers.put(id, this);
        return workers;
    }

    private List<Object> splitPayload(String jobType, Object payload, int parts) {
        List<Object> chunks = new ArrayList<>(WorkerTaskDistributor.split(jobType, payload, parts));
        // Pad with nulls so chunk i always belongs to participant i.
        while (chunks.size() < parts) {
            chunks.add(null);
        }
        return chunks;
    }

    // Combines numeric partials; a non-numeric partial is passed through as-is.
    private Object aggregateJob(String jobType, int jobId) {
        Map<Integer, Object> results = getJobResults(jobId);
        List<Number> partialResults = new ArrayList<>(results.size());
        for (Object result : results.values()) {
            if (!(result instanceof Number number)) {
                return result;
            }
            partialResults.add(number);
        }
        try {
            return WorkerTaskDistributor.combine(jobType, partialResults);
        } catch (ArithmeticException | IllegalArgumentException e) {
            return "Error aggregating task: " + e.getMessage();
        }
    }

    // Calls the client back on its own registry; a dead client must not fail the job.
    private void deliverToClient(int jobId, Object result, JobState state) {
        try {
            Registry clientRegistry = LocateRegistry.getRegistry(state.clientHost(), state.clientPort());
            ClientRemote client = (ClientRemote) clientRegistry.lookup(state.clientName());
            client.receiveJobResult(jobId, result);
        } catch (Exception e) {
            System.out.println("Worker " + id + ": could not deliver result for job " + jobId
                    + " to client : " + e.getMessage());
        }
    }

    // Resets the budget and starts the next election in one CAS, so one thread hands over.
    private void maybeRotateCoordinator() {
        while (true) {
            int used = termJobsCoordinated.get();
            if (used < Constants.JOBS_PER_TERM) {
                return;
            }
            // One step means exactly one handover; a loser retries and sees the reset.
            if (termJobsCoordinated.compareAndSet(used, 0)) {
                handoverInProgress.set(true);
                // Gate submitJob before the election, closing the window where a term overserves.
                startElection();
                return;
            }
        }
    }

    // Join point: store the directory and start probing the coordinator.
    public void setBootstrapReference(BootstrapRemote bootstrap, String localHost) {
        this.bootstrap = bootstrap;
        this.localHost = localHost;
        heartbeatExecutor.scheduleAtFixedRate(this::heartbeatTick,
                Constants.HEARTBEAT_INTERVAL_MS, Constants.HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void ping() throws RemoteException {
    }

    // Probes the coordinator; an unanswered ping means failure, so start an election.
    private void heartbeatTick() {
        int coord = currentCoordinatorId;
        if (coord == -1 || coord == id || bootstrap == null) {
            return;
        }
        // One probe at a time, so a slow coordinator cannot pile up overlapping calls.
        if (!heartbeatInFlight.compareAndSet(false, true)) {
            return;
        }
        try {
            WorkerRemote stub = resolveCoordinatorStub(coord);
            if (stub == null) {
                return;
            }
            try {
                stub.ping();
            } catch (RemoteException e) {
                coordinatorStub = null;
                // Drop the cached stub so the next tick re-resolves it from the directory.
                startElection();
            }
        } catch (Exception e) {
            coordinatorStub = null;
            startElection();
        } finally {
            heartbeatInFlight.set(false);
        }
    }

    // Reuse the cached stub while the coordinator is unchanged, else look it up again.
    private WorkerRemote resolveCoordinatorStub(int coord) {
        if (coordinatorStub != null && cachedCoordinatorId == coord) {
            return coordinatorStub;
        }
        try {
            WorkerAddress address = bootstrap.getWorkerAddress(coord);
            if (address == null) {
                handleDeadNeighbour(coord);
                return null;
            }
            Registry registry = LocateRegistry.getRegistry(address.getRmiHost(), address.getRmiPort());
            WorkerRemote stub = (WorkerRemote) registry.lookup("Worker" + coord);
            coordinatorStub = stub;
            cachedCoordinatorId = coord;
            return stub;
        } catch (Exception e) {
            return null;
        }
    }

    // Stops both pools; daemon threads mean a missed call cannot block JVM exit.
    public void shutdown() {
        executor.shutdown();
        heartbeatExecutor.shutdown();
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

    public Object getTaskResult(int jobId, int workerId) {
        return taskResults.get(new JobResultKey(jobId, workerId));
    }

    public Map<Integer, Object> getJobResults(int jobId) {
    // Flattens the composite key into workerId -> result for a single job.
        Map<Integer, Object> out = new HashMap<>();
        for (Map.Entry<JobResultKey, Object> e : taskResults.entrySet()) {
            if (e.getKey().jobId() == jobId) {
                out.put(e.getKey().workerId(), e.getValue());
            }
        }
        return out;
    }

    public int getActiveTaskCount() {
        return activeTasks.size();
    }
}