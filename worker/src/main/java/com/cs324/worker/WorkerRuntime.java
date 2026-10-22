package com.cs324.worker;

import com.cs324.common.ClusterDirectory;
import com.cs324.common.BoundedMap;
import com.cs324.common.BoundedSet;
import com.cs324.common.Constants;
import com.cs324.common.WorkerControl;
import com.cs324.common.WorkerEvent;
import com.cs324.election.ElectionGrpcService;
import com.cs324.election.ElectionHandler;
import com.cs324.election.ElectionTransport;
import com.cs324.election.GrpcElectionTransport;
import com.cs324.election.proto.CoordinatorAnnouncement;
import com.cs324.election.proto.EchoMessage;
import com.cs324.election.proto.ElectionMessage;
import com.cs324.failover.CheckpointLog;
import com.cs324.failover.CheckpointReplicator;
import com.cs324.failover.CheckpointSink;
import com.cs324.failover.CoordinatorHealthMonitor;
import com.cs324.failover.FailoverCoordinator;
import com.cs324.failover.FailoverGrpcService;
import com.cs324.failover.FailoverHandler;
import com.cs324.failover.FailureGate;
import com.cs324.failover.GrpcCoordinatorProbe;
import com.cs324.failover.JobCheckpoint;
import com.cs324.failover.LeaderView;
import com.cs324.failover.RestoredJob;
import com.cs324.failover.proto.CheckpointEntry;
import com.cs324.failover.proto.HeartbeatReply;
import com.cs324.job.proto.TaskResult;
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

public class WorkerRuntime implements WorkerControl, ElectionHandler, FailoverHandler {
    private final com.cs324.progress.ProgressService progress = new com.cs324.progress.ProgressService();
    private io.grpc.Server progressServer;
    private volatile int grpcPort = -1;
    private com.cs324.computation.RegistryClient grpcDirectory;
    private com.cs324.computation.GrpcJobService grpcJobs;
    private String grpcHost = "localhost";
    private volatile int leaseContactPort;

    // All peer election hops use the published gRPC endpoint.
    private volatile ElectionTransport electionTransport;

    // Monotonic leadership generation carried by every election message.
    private final AtomicLong currentTerm = new AtomicLong(0);

    // Every worker retains checkpoints so a survivor can restore jobs after coordinator failure.
    private final CheckpointLog checkpoints;
    private volatile CheckpointReplicator replicator;
    private volatile CoordinatorHealthMonitor healthMonitor;
    private volatile GrpcCoordinatorProbe probe;
    private volatile FailoverCoordinator failover;
    private volatile QuorumMonitor quorumMonitor;
    private volatile QuorumProbe quorumProbe;
    // Refreshes this worker's membership lease without allocating a new identity.
    private volatile ScheduledExecutorService bootstrapLease;
    // Lease bookkeeping: a leader may serve only while its last majority check is fresh.
    private final AtomicLong lastQuorumMillis = new AtomicLong(0);
    private final AtomicBoolean quorumHealthy = new AtomicBoolean(false);
    // Block admission during recovery to prevent accepting a job that is also being restored.
    private final AtomicBoolean failoverInProgress = new AtomicBoolean(false);
    // One election per detected failure; the gate holds the window and the key.
    private final FailureGate failureGate =
            new FailureGate(Constants.FAILOVER_SUPPRESSION_MS, System::currentTimeMillis);

    // Share one listener for computation, progress and election, preserving term limits and JAC accounting.
    public synchronized void startProgressServer() throws java.io.IOException {
        if (progressServer == null) {
            grpcJobs = new com.cs324.computation.GrpcJobService(id,
                    () -> com.cs324.job.proto.CoordinatorEndpoint.newBuilder().setWorkerId(id)
                            .setHost(grpcHost).setPort(getProgressPort()).build(),
                    () -> {
                        if (grpcDirectory == null) throw new IllegalStateException("gRPC discovery is not configured");
                        return grpcDirectory.workers();
                    }, jobId -> {
                        // Reject new jobs until checkpoint recovery finishes.
                        if (currentCoordinatorId != id || handoverInProgress.get() || failoverInProgress.get()) {
                            return false;
                        }
                        try { reserveTermSlot(jobId); return true; }
                        catch (IllegalStateException e) { return false; }
                    }, workerId -> {
                        jobAllocationCounter.incrementAndGet();
                    }, this::maybeRotateCoordinator, executor, progress, java.time.Duration.ofMinutes(10), this::recordEvent);
            grpcJobs.setCheckpointSink(new ReplicatingCheckpointSink());
            grpcJobs.setAdmissionRollback(this::releaseTermSlot);
            progressServer = io.grpc.ServerBuilder.forPort(0).addService(progress).addService(grpcJobs)
                    .addService(new com.cs324.computation.WorkerControlService(this))
                    .addService(new ElectionGrpcService(this))
                    .addService(new FailoverGrpcService(this, id)).build().start();
            // Cache the bound port because getPort() is unavailable after server shutdown.
            grpcPort = progressServer.getPort();
        }
    }

    // Publish the actual bound port after registration so peers can connect.
    public void enableGrpcJobs(String bootstrapHost, int bootstrapGrpcPort, String host, int contactPort) {
        grpcHost = host;
        leaseContactPort = contactPort;
        grpcDirectory = new com.cs324.computation.RegistryClient(bootstrapHost, bootstrapGrpcPort);
        grpcDirectory.publish(id, host, contactPort, getProgressPort());
        // From here on the election wave travels over gRPC, resolved through the same directory.
        useGrpcElectionTransport(grpcDirectory);
        startFailoverServices();
        System.out.println("Worker " + id + " gRPC jobs and progress on port " + getProgressPort());
    }

    // Start replication, health checks and recovery once peer discovery is available.
    private synchronized void startFailoverServices() {
        if (replicator != null) {
            return;
        }
        replicator = new CheckpointReplicator(id, grpcDirectory, executor, this::logFailover);
        failover = new FailoverCoordinator(id, checkpoints, replicator,
                (job, remainingMillis) -> grpcJobs != null && grpcJobs.restore(job, remainingMillis),
                job -> grpcJobs != null && grpcJobs.deliverResult(job.request(), job.finalResult()),
                currentTerm::get, java.time.Duration.ofMinutes(10).toMillis(), this::logFailover);
        probe = new GrpcCoordinatorProbe(id, grpcDirectory, currentTerm::get,
                () -> currentCoordinatorId, this::logFailover);
        healthMonitor = new CoordinatorHealthMonitor(id, () -> currentCoordinatorId, probe,
                this::requestFailoverElection, this::observeLeadershipDrift, this::logFailover);
        healthMonitor.start();
        quorumProbe = new QuorumProbe(grpcDirectory, Constants.QUORUM_PROBE_DEADLINE_MS);
        quorumMonitor = new QuorumMonitor(id, () -> currentCoordinatorId == id,
                this::readyMemberIds, quorumProbe::reachable, this::markQuorum, this::onQuorumLost,
                this::logFailover, Constants.QUORUM_LOSS_THRESHOLD, Constants.QUORUM_CHECK_INTERVAL_MS);
        quorumMonitor.start();
        startBootstrapLease();
    }

    // Refresh this worker membership lease even when it has no jobs.
    private synchronized void startBootstrapLease() {
        if (bootstrapLease != null) {
            return;
        }
        bootstrapLease = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "worker-bootstrap-lease");
            thread.setDaemon(true);
            return thread;
        });
        bootstrapLease.scheduleWithFixedDelay(() -> {
            if (grpcDirectory == null) {
                return;
            }
            try {
                grpcDirectory.publish(id, grpcHost, leaseContactPort, getProgressPort());
            } catch (RuntimeException e) {
                recordEvent(WorkerEvent.FAILOVER_DETAIL,
                        "Bootstrap lease refresh failed: " + e.getMessage());
            }
        }, Constants.BOOTSTRAP_LEASE_INTERVAL_MS, Constants.BOOTSTRAP_LEASE_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    // Workers that have published a gRPC endpoint; these are the cluster's voting members.
    private Set<Integer> readyMemberIds() {
        Set<Integer> members = new java.util.HashSet<>();
        if (grpcDirectory != null) {
            for (var endpoint : grpcDirectory.workers()) {
                if (endpoint.getWorkerId() > 0) {
                    members.add(endpoint.getWorkerId());
                }
            }
        }
        members.add(id);
        return members;
    }

    private void markQuorum() {
        lastQuorumMillis.set(System.currentTimeMillis());
        quorumHealthy.set(true);
    }

    // A leader without a majority must stop serving, or a partitioned cluster has two of them.
    private synchronized void onQuorumLost() {
        quorumHealthy.set(false);
        lastQuorumMillis.set(0L);
        if (currentCoordinatorId == id) {
            currentCoordinatorId = -1;
            handoverInProgress.set(false);
            failoverInProgress.set(false);
            recordEvent(WorkerEvent.QUORUM_LOST, "Worker " + id + " lost its majority quorum");
            recordEvent(WorkerEvent.LEADERSHIP_STEP_DOWN,
                    "Worker " + id + " stepped down: fewer than a majority of workers are reachable");
        }
    }

    private boolean hasQuorumLease() {
        return quorumHealthy.get()
                && System.currentTimeMillis() - lastQuorumMillis.get() <= Constants.QUORUM_LEASE_MS;
    }

    // Failover bookkeeping is operational detail; the feed keeps it, stdout stays clean.
    private void logFailover(String message) {
        recordEvent(WorkerEvent.FAILOVER_DETAIL, message);
    }

    // Switches election hops to gRPC. Idempotent, so a re-published endpoint is harmless.
    private synchronized void useGrpcElectionTransport(com.cs324.computation.RegistryClient directory) {
        ElectionTransport current = electionTransport;
        if (current instanceof GrpcElectionTransport) {
            return;
        }
        if (current != null) {
            current.close();
        }
        electionTransport = new GrpcElectionTransport(id, directory, Constants.ELECTION_RPC_DEADLINE_MS,
                this::logElectionTransport);
    }

    private ElectionTransport electionTransport() {
        if (electionTransport == null) throw new IllegalStateException("gRPC discovery is not configured");
        return electionTransport;
    }

    // Transport faults are already handled where they happen, so this only keeps the feed honest.
    private void logElectionTransport(String message) {
        recordEvent(WorkerEvent.ELECTION_TRANSPORT, message);
    }

    @Override
    public synchronized int getProgressPort() { return grpcPort; }

    // One election wave, plus the bookkeeping needed to complete it.
    private static class ElectionState {
        // Neighbour that forwarded this wave, so results can be echoed back to it.
        int parentId;
        // Best candidate in this wave so far, compared by JAC then by id.
        int bestCandidateId;
        int bestCandidateJac;
        // Highest term seen in this wave, so the root announces above everything it heard.
        long maxTerm;
        // Neighbours that have not answered yet; the wave completes when this empties.
        final Set<Integer> pendingChildren = ConcurrentHashMap.newKeySet();
        // Every worker this wave has reached, unioned as echoes travel back to the root.
        final Set<Integer> voters = ConcurrentHashMap.newKeySet();
        // Latch so a wave echoes upward exactly once, however many triggers fire.
        boolean echoed = false;

        ElectionState(int parentId, int bestCandidateId, int bestCandidateJac, long term) {
            this.parentId = parentId;
            this.bestCandidateId = bestCandidateId;
            this.bestCandidateJac = bestCandidateJac;
            this.maxTerm = term;
        }
    }


    private final int id;
    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private final AtomicLong electionCounter = new AtomicLong(0);
    private volatile int currentCoordinatorId = -1;
    private final BoundedSet<Long> seenMessageIds = new BoundedSet<>(Constants.ELECTION_MESSAGE_RETENTION);
    private final BoundedSet<Long> seenCoordinatorMessageIds = new BoundedSet<>(Constants.ELECTION_MESSAGE_RETENTION);
    private final BoundedMap<Long, ElectionState> elections = new BoundedMap<>(Constants.ELECTION_STATE_RETENTION);
    private final Map<Integer, WorkerControl> neighbours = new ConcurrentHashMap<>();
    // Last-known neighbour set per direct neighbour (2-hop view for grandparent rewire).
    private final Map<Integer, Set<Integer>> twoHop = new ConcurrentHashMap<>();
    // Once-guard so concurrent failures on the same dead id rewire exactly once.
    private final Set<Integer> repairInProgress = ConcurrentHashMap.newKeySet();
    // A repaired edge is not part of the wave that discovered the dead one; retry once with it.
    private final AtomicBoolean electionRetryScheduled = new AtomicBoolean(false);
    // Recent happenings for the GUI feed; a bounded ring so a chatty cluster cannot leak memory.
    private final Deque<WorkerEvent> recentEvents = new ArrayDeque<>();
    // Finer job slices produce more per-task events; keep enough history that the
    // failure, recovery and result of one job remain visible in the GUI feed.
    private static final int MAX_RECENT_EVENTS = 256;
    private final AtomicLong eventSeq = new AtomicLong(0);

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
    // Bootstrap directory, set once on join; null in tests so we fall back to peers.
    private volatile ClusterDirectory bootstrap;
    // Local host as published to peers; set once on join for repair reverse edges.
    private volatile String localHost;
    // Reserve term slots under the worker lock, up to JOBS_PER_TERM.
    private final AtomicInteger termJobsCoordinated = new AtomicInteger(0);
    private final Map<Integer, Long> termReservations = new HashMap<>();
    // Closed between winning a rotation and the next coordinator announcement.
    private final AtomicBoolean handoverInProgress = new AtomicBoolean(false);

    public WorkerRuntime(int id) {
        this.id = id;
        this.checkpoints = new CheckpointLog(id);
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public int getJobAllocationCounter() {
        return jobAllocationCounter.get();
    }

    @Override
    public int getCurrentCoordinatorId() {
        return currentCoordinatorId;
    }

    @Override
    public Set<Integer> getNeighbourIds() {
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
    public List<WorkerEvent> getRecentEvents(long afterSeq) {
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

    // On graceful exit, give neighbours the links needed to reconnect; crashes rely on failure detection.
    public int notifyNeighboursOfLeaving() {
        Set<Integer> mine = Set.copyOf(neighbours.keySet());
        int notified = 0;
        for (WorkerControl peer : new ArrayList<>(neighbours.values())) {
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
    public boolean isActive() {
        return true;
    }

    
    // Starts a new wave: we are the origin and begin as our own best candidate.
    @Override
    public long startElection() {
        // Worker id in the high bits keeps message ids globally unique without a registry.
        long messageId = ((long) id << 32) | (electionCounter.getAndIncrement());
        seenMessageIds.add(messageId);

        int currentJac = jobAllocationCounter.get();
        long term = currentTerm.get();
        ElectionState election = new ElectionState(this.id, this.id, currentJac, term);
        election.voters.add(this.id);
        recordEvent(WorkerEvent.ELECTION_STARTED,
                "started election wave " + messageId + " (JAC " + currentJac + ", term " + term + ")");

        for (Integer neighborId : neighbours.keySet()) {
            election.pendingChildren.add(neighborId);
        }

        elections.put(messageId, election);

        ElectionTransport transport = electionTransport();
        for (Integer neighborId : new ArrayList<>(neighbours.keySet())) {
            if (!transport.sendElection(neighborId, this.id, this.id, currentJac, messageId, term,
                    Set.copyOf(election.voters))) {
                election.pendingChildren.remove(neighborId);
                handleDeadNeighbour(neighborId);
            }
        }

        // No reachable neighbour, so this wave is already complete and echoes immediately.
        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
        return messageId;
    }

    // gRPC wave hop; the algorithm below is the one place the protocol lives.
    @Override
    public void onElectionMessage(ElectionMessage message) {
        applyElectionMessage(message.getOriginId(), message.getCandidateId(), message.getCandidateJac(),
                message.getMessageId(), message.getTerm(), message.getVotersList());
    }

    // Folds an incoming wave in: we win on lower JAC, then on higher id.
    private void applyElectionMessage(int originId, int candidateId, int candidateJac, long messageId, long term,
            List<Integer> incomingVoters) {
        // A hop below our own term belongs to a superseded wave; answering it would only revive a coordinator the cluster has already moved past.
        if (term > 0 && term < currentTerm.get()) {
            return;
        }
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
                existing.voters.addAll(incomingVoters);
                existing.voters.add(WorkerRuntime.this.id);
                alreadySeen[0] = existing;
                return existing;
            }
            ElectionState election = new ElectionState(parentId, ourBestId, ourBestJac, term);
            election.voters.addAll(incomingVoters);
            election.voters.add(WorkerRuntime.this.id);
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
            ElectionTransport transport = electionTransport();
            if (!transport.sendEcho(parentId, messageId, this.id, best[0], best[1], term,
                    Set.copyOf(seen.voters))) {
                handleDeadNeighbour(parentId);
            }
            return;
        }

        ElectionState election = created[0];
        // Lazy 2-hop seed: first wave from a new parent teaches us its neighbour set.
        if (!twoHop.containsKey(parentId)) {
            refreshTwoHop(parentId, neighbours.get(parentId));
        }

        // Forward to every neighbour except the one this wave arrived from.
        ElectionTransport transport = electionTransport();
        for (Integer neighborId : new ArrayList<>(neighbours.keySet())) {
            if (neighborId != parentId) {
                if (!transport.sendElection(neighborId, this.id, ourBestId, ourBestJac, messageId, term,
                        Set.copyOf(election.voters))) {
                    // Unreachable neighbour: stop waiting on it and rewire past it.
                    election.pendingChildren.remove(neighborId);
                    handleDeadNeighbour(neighborId);
                }
            }
        }

        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
    }

    // gRPC announcement hop.
    @Override
    public void onCoordinatorAnnouncement(CoordinatorAnnouncement announcement) {
        applyCoordinatorAnnouncement(announcement.getCoordinatorId(), announcement.getMessageId(),
                announcement.getTerm());
    }

    // Applies a coordinator announcement and relays it onward to the whole network.
    private void applyCoordinatorAnnouncement(int coordinatorId, long messageId, long term) {
        // First sighting; a broadcast that loops back to us stops here.
        if (!seenCoordinatorMessageIds.add(messageId)) {
            return;
        }
        if (!acceptLeadership(coordinatorId, term, "announced")) {
            return;
        }

        ElectionTransport transport = electionTransport();
        for (Integer neighborId : new ArrayList<>(neighbours.keySet())) {
            if (!transport.sendAnnouncement(neighborId, coordinatorId, messageId, term)) {
                handleDeadNeighbour(neighborId);
            }
        }
    }

    // Apply only a newer (term, coordinator ID) leadership view.
    private synchronized boolean acceptLeadership(int coordinatorId, long term, String cause) {
        if (term <= 0) return false;
        {
            long heldTerm = currentTerm.get();
            if (term < heldTerm) {
                return false;
            }
            if (term == heldTerm && coordinatorId <= currentCoordinatorId) {
                return false;
            }
        }
        boolean changed = this.currentCoordinatorId != coordinatorId || (term > 0 && term > currentTerm.get());
        this.currentCoordinatorId = coordinatorId;
        if (term > currentTerm.get()) {
            currentTerm.set(term);
        }
        handoverInProgress.set(false);
        if (coordinatorId == this.id) {
            termJobsCoordinated.set(0);
            termReservations.clear();
            // Start a short quorum lease after election; the quorum monitor renews or revokes it.
            quorumHealthy.set(true);
            lastQuorumMillis.set(System.currentTimeMillis());
        }
        if (cause.equals("announced") && coordinatorId != this.id) {
            recordEvent(WorkerEvent.COORDINATOR_ANNOUNCED, "coordinator is now Worker " + coordinatorId
                    + " (term " + term + ")");
        }
        // Winning is the moment to find out what the previous coordinator was holding.
        if (changed && coordinatorId == this.id) {
            takeOverLeadership();
        }
        return true;
    }

    // gRPC echo hop.
    @Override
    public void onEchoMessage(EchoMessage message) {
        applyEchoMessage(message.getMessageId(), message.getFromWorkerId(), message.getBestCandidateId(),
                message.getBestCandidateJac(), message.getTerm(), message.getVotersList());
    }

    // Folds a child's answer in and completes the wave once nobody is outstanding.
    private void applyEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac,
            long term, List<Integer> incomingVoters) {
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
            // The root announces above every term the wave saw, so its verdict cannot be mistaken for one of the older waves still echoing back.
            election.maxTerm = Math.max(election.maxTerm, term);
            election.voters.addAll(incomingVoters);
            election.voters.add(WorkerRuntime.this.id);

            election.pendingChildren.remove(fromWorkerId);

                // Last outstanding child: complete the wave. The echoed latch keeps it once-only.
            if (election.pendingChildren.isEmpty() && !election.echoed) {
                propagateEcho(messageId, election);
            }
        }
    }

    // The root announces the winner; every other node relays the result upward.
    private void propagateEcho(long messageId, ElectionState election) {
        final Set<Integer> voters;
        synchronized (election) {
            // Set under the lock, so concurrent triggers cannot announce twice.
            if (election.echoed) {
                return;
            }
            election.echoed = true;
            voters = Set.copyOf(election.voters);
        }

        ElectionTransport transport = electionTransport();
        // We started this wave, so we are the root and decide the outcome.
        if (election.parentId == this.id) {
            Set<Integer> members;
            try {
                members = readyMemberIds();
            } catch (RuntimeException directoryUnavailable) {
                recordEvent(WorkerEvent.ELECTION_NO_QUORUM, "wave " + messageId
                        + " could not read membership: " + directoryUnavailable.getMessage());
                if (currentCoordinatorId == id) {
                    onQuorumLost();
                }
                return;
            }
            if (!hasMajority(voters, members)) {
                recordEvent(WorkerEvent.ELECTION_NO_QUORUM, "wave " + messageId + " reached "
                        + voters.size() + " of " + members.size() + " ready workers; no winner announced");
                if (currentCoordinatorId == id) {
                    onQuorumLost();
                }
                return;
            }
            // One above the highest term this wave carried, so every node can order the verdict.
            long announceTerm = Math.max(currentTerm.get(), election.maxTerm) + 1;
            acceptLeadership(election.bestCandidateId, announceTerm, "won");
            recordEvent(WorkerEvent.COORDINATOR_ANNOUNCED, "announced coordinator Worker "
                    + election.bestCandidateId + " (wave " + messageId + ", term " + announceTerm + ")");
            // Offset, so a coordinator announcement can never collide with an election id.
            long coordMessageId = messageId + 1000000L;
            // Record before broadcasting, or our own announcement returns and resets the term.
            seenCoordinatorMessageIds.add(coordMessageId);
            for (Integer neighborId : new ArrayList<>(neighbours.keySet())) {
                if (!transport.sendAnnouncement(neighborId, election.bestCandidateId, coordMessageId, announceTerm)) {
                    handleDeadNeighbour(neighborId);
                }
            }
        } else {
            // Send our merged best back to the node that forwarded this wave.
            if (!transport.sendEcho(election.parentId, messageId, this.id,
                    election.bestCandidateId, election.bestCandidateJac, election.maxTerm, voters)) {
                handleDeadNeighbour(election.parentId);
            }
        }
    }

    private static boolean hasMajority(Set<Integer> voters, Set<Integer> members) {
        long recognisedVotes = voters.stream().filter(members::contains).count();
        return !members.isEmpty() && recognisedVotes >= members.size() / 2 + 1;
    }

    @Override
    public void connectAsNeighbour(int peerId, String host, int port) {
        connectGrpcNeighbour(peerId);
    }

    public void connectGrpcNeighbour(int peerId) {
        if (peerId == id) return;
        if (grpcDirectory == null) throw new IllegalStateException("gRPC discovery is not ready");
        var peer = new com.cs324.computation.GrpcWorkerClient(grpcDirectory.worker(peerId));
        var status = peer.status();
        if (status.getWorkerId() != peerId) throw new IllegalStateException("Peer identity mismatch");
        neighbours.put(peerId, peer);
        twoHop.put(peerId, Set.copyOf(status.getNeighbourIdsList()));
        recordEvent(WorkerEvent.NEIGHBOUR_ADDED, "linked Worker " + peerId + " via gRPC");
    }

    public void adoptGrpcCoordinator(int coordinatorId, long term) {
        if (coordinatorId > 0) acceptLeadership(coordinatorId, term, "adopted");
    }

    @Override
    // Give neighbours this worker links so they can reconnect after a graceful exit.
    public void notifyLeaving(int leaverId, Set<Integer> leaverNeighbours) {
        if (leaverNeighbours != null) {
            twoHop.put(leaverId, Set.copyOf(leaverNeighbours));
        }
        handleDeadNeighbour(leaverId, "leave");
    }

    // Best-effort fetch of a neighbour's neighbour set; a failure means the peer just died.
    private void refreshTwoHop(int neighbourId, WorkerControl stub) {
        if (stub == null) {
            // No stub means no link to learn from, which is not a failure to report.
            return;
        }
        try {
            Set<Integer> ids = stub.getNeighbourIds();
            if (ids != null) {
                twoHop.put(neighbourId, Set.copyOf(ids));
            }
        } catch (Exception ignored) {
            handleDeadNeighbour(neighbourId);
        }
    }

    // Replace a dead edge with a reachable grandparent, falling back to a directory peer.
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
                    retryElectionAfterRepair(deadId);
                    return;
                }
            }
            // No grandparent worked: ask the directory for any other live worker.
            try {
                List<Integer> active = directoryMembers();
                Collections.sort(active);
                for (int candidate : active) {
                    if (isRepairCandidate(candidate, deadId) && tryRepairEdge(candidate, deadId)) {
                        retryElectionAfterRepair(deadId);
                        return;
                    }
                }
            } catch (Exception ignored) {
                // directory unreachable: keep the pruned state, a later wave retries
            }
        } finally {
            repairInProgress.remove(deadId);
        }
    }

    // A new edge is added after the failed wave has already chosen its voters. If the
    // dead worker was our coordinator, run one more wave now that the survivors can
    // reach each other; otherwise a star rooted at the dead hub stays leaderless.
    private void retryElectionAfterRepair(int deadId) {
        if (currentCoordinatorId != -1 && currentCoordinatorId != deadId) {
            return;
        }
        if (!electionRetryScheduled.compareAndSet(false, true)) {
            return;
        }
        Thread retry = new Thread(() -> {
            try {
                long jitter = Constants.FAILOVER_ELECTION_JITTER_MS <= 0 ? 0
                        : (long) (Math.random() * Constants.FAILOVER_ELECTION_JITTER_MS);
                if (jitter > 0) {
                    Thread.sleep(jitter);
                }
                if (!isShutdown() && (currentCoordinatorId == -1 || currentCoordinatorId == deadId)) {
                    startElection();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                electionRetryScheduled.set(false);
            }
        }, "election-retry-" + id);
        retry.setDaemon(true);
        retry.start();
    }

    // Eligible rewire target: not us, not the corpse, and not already a neighbour.
    private boolean isRepairCandidate(int candidate, int deadId) {
        return candidate != id && candidate != deadId && !neighbours.containsKey(candidate);
    }

    // Workers the directory can reach. The gRPC registry is the source of truth, so the
    // repair works for every worker that published an endpoint, not only for the ones
    // that were handed a directory reference at startup.
    private List<Integer> directoryMembers() {
        if (grpcDirectory != null) {
            List<Integer> members = new ArrayList<>();
            for (var endpoint : grpcDirectory.workers()) {
                members.add(endpoint.getWorkerId());
            }
            return members;
        }
        if (bootstrap != null) {
            return new ArrayList<>(bootstrap.getActiveWorkerIds());
        }
        return List.of();
    }

    // Resolves one candidate through the registry and links it bidirectionally; false = try next.
    private boolean tryRepairEdge(int candidate, int deadId) {
        if (grpcDirectory == null) {
            return false;
        }
        try {
            WorkerControl stub = new com.cs324.computation.GrpcWorkerClient(grpcDirectory.worker(candidate));
            stub.ping();
            neighbours.put(candidate, stub);
            refreshTwoHop(candidate, stub);
            recordEvent(WorkerEvent.NEIGHBOUR_ADDED,
                    "chained to Worker " + candidate + " past Worker " + deadId);
            try {
                stub.connectAsNeighbour(id, localHostOrDefault(), getProgressPort());
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

    // Reserves one slot in the term; returns true when that job spent the budget.
    private synchronized boolean reserveTermSlot(int jobId) {
        if (currentCoordinatorId != id || handoverInProgress.get() || failoverInProgress.get()
                || !hasQuorumLease())
            throw new IllegalStateException("Worker is not accepting coordinator jobs");
        while (true) {
            int used = termJobsCoordinated.get();
            if (used >= Constants.JOBS_PER_TERM) {
                throw new IllegalStateException("Worker " + id + " has coordinated the maximum of "
                        + Constants.JOBS_PER_TERM + " jobs for this term; it is handing over leadership");
            }
            // Increment and check are one CAS, so the count is never observable above the cap.
            if (termJobsCoordinated.compareAndSet(used, used + 1)) {
                termReservations.put(jobId, currentTerm.get());
                return used + 1 == Constants.JOBS_PER_TERM;
            }
        }
    }

    private synchronized void releaseTermSlot(int jobId) {
        Long reservedTerm = termReservations.remove(jobId);
        if (reservedTerm != null && reservedTerm == currentTerm.get() && currentCoordinatorId == id)
            termJobsCoordinated.updateAndGet(count -> Math.max(0, count - 1));
    }

    // Closes admission and resets the budget atomically, so one thread hands over.
    private void maybeRotateCoordinator() {
        synchronized (this) {
            if (termJobsCoordinated.get() < Constants.JOBS_PER_TERM || handoverInProgress.get()) return;
            // Reset the budget and close admission under the same lock to prevent an extra reservation.
            handoverInProgress.set(true);
            termJobsCoordinated.set(0);
        }
        // Network calls run outside the admission/leadership lock.
        startElection();
    }

    // Join point: store the directory and start probing the coordinator.
    public void setBootstrapReference(ClusterDirectory bootstrap, String localHost) {
        this.bootstrap = bootstrap;
        this.localHost = localHost;
    }

    // This worker's own leadership view, sent as the reply to a follower probing it.
    @Override
    public HeartbeatReply heartbeat(int fromWorkerId, long followerTerm, int followerCoordinatorId) {
        return HeartbeatReply.newBuilder()
                .setTerm(currentTerm.get())
                .setCoordinatorId(currentCoordinatorId)
                .setIsCoordinator(currentCoordinatorId == id)
                .build();
    }

    // Folds entries a coordinator replicated to us; the sequence and term rules do the work.
    @Override
    public long acceptEntries(int coordinatorId, long term, List<CheckpointEntry> entries) {
        long highest = 0L;
        for (CheckpointEntry entry : entries) {
            if (checkpoints.apply(entry)) {
                highest = Math.max(highest, entry.getSequence());
            } else {
                // Acknowledge already-stored state so the sender does not retry duplicate entries.
                highest = Math.max(highest, checkpoints.job(entry.getJobId()) == null
                        ? 0L : checkpoints.job(entry.getJobId()).sequence());
            }
        }
        return highest;
    }

    // Include retained terminal markers so a stale survivor never resurrects a finished job.
    @Override
    public List<CheckpointEntry> snapshots() {
        List<CheckpointEntry> entries = new ArrayList<>();
        for (int jobId : checkpoints.jobIds()) {
            entries.addAll(checkpoints.snapshot(jobId));
        }
        return entries;
    }

    // The local replica, for tests and for the takeover's own merge.
    public CheckpointLog checkpointLog() {
        return checkpoints;
    }

    // Suppress repeated local failure elections and add jitter before starting a wave.
    private void requestFailoverElection(int failedCoordinator) {
        if (!failureGate.shouldElect(failedCoordinator)) {
            return;
        }
        long jitter = Constants.FAILOVER_ELECTION_JITTER_MS <= 0 ? 0
                : (long) (Math.random() * Constants.FAILOVER_ELECTION_JITTER_MS);
        recordEvent(WorkerEvent.FAILOVER_STARTED, "coordinator Worker " + failedCoordinator
                + " declared failed; starting an election in " + jitter + "ms");
        try {
            if (jitter > 0) {
                Thread.sleep(jitter);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        startElection();
    }

// Adopt a newer heartbeat view and step down if this worker no longer leads.
    private void observeLeadershipDrift(CoordinatorHealthMonitor.ProbeResult view) {
        long peerTerm = view.term() == null ? 0L : view.term();
        LeaderView.Decision decision = LeaderView.decide(currentTerm.get(), currentCoordinatorId,
                peerTerm, view.coordinatorIdView(), view.isCoordinator());
        if (!decision.adopt()) {
            return;
        }
        boolean wasCoordinator = currentCoordinatorId == id;
        if (acceptLeadership(decision.coordinatorId(), decision.term(), "drift")
                && wasCoordinator && decision.coordinatorId() != id) {
            recordEvent(WorkerEvent.LEADERSHIP_STEP_DOWN, "Worker " + id + " stepped down for Worker "
                    + decision.coordinatorId() + " at term " + decision.term());
        }
    }

    // Block new submissions while the new coordinator merges replicas and restores orphaned jobs.
    private void takeOverLeadership() {
        if (failover == null) {
            return;
        }
        failoverInProgress.set(true);
        try {
            executor.execute(() -> {
                try {
                    failover.takeOver();
                } catch (RuntimeException e) {
                    recordEvent(WorkerEvent.FAILOVER_COMPLETED, "takeover stopped early: " + e.getMessage());
                } finally {
                    failoverInProgress.set(false);
                }
            });
        } catch (RejectedExecutionException e) {
            // Shutting down: the next election will take over instead.
            failoverInProgress.set(false);
        }
    }

    // Record transitions locally and on peers; only acceptance waits for a confirmed replica.
    private final class ReplicatingCheckpointSink implements CheckpointSink {

        @Override
        public void onAccepted(com.cs324.job.proto.SubmitJobRequest request,
                List<SliceSnapshot> slices, long acceptedAtMillis) {
            write(() -> checkpoints.acceptedEntry(request, slices, acceptedAtMillis,
                    currentTerm.get(), id), true);
        }

        @Override
        public void onSliceUpdated(int jobId, int taskId, com.cs324.registry.proto.WorkerEndpoint owner,
                int attempt, TaskResult result) {
            write(() -> checkpoints.sliceEntry(jobId, taskId, owner, attempt, result, currentTerm.get(), id), false);
        }

        @Override
        public void onFinished(int jobId, TaskResult result) {
            write(() -> checkpoints.finishedEntry(jobId, result, false, currentTerm.get(), id), false);
        }

        @Override
        public void onDelivered(int jobId) {
            write(() -> {
                JobCheckpoint job = checkpoints.job(jobId);
                return job != null && job.finished()
                        ? checkpoints.finishedEntry(jobId, job.finalResult(), true, currentTerm.get(), id) : null;
            }, false);
        }

        @Override
        public void onDeliveryFailed(int jobId) {
            checkpoints.markDelivered(jobId, false);
        }

        @Override
        public void onOwnershipClaimed(int jobId) {
            // The claim is pushed to the peers so no other survivor rebuilds this job.
            // It is best effort: a failed push must not lose the recovery itself.
            write(() -> checkpoints.claimEntry(jobId, id, currentTerm.get()), false);
        }

        @Override
        public boolean ownsJob(int jobId) {
            JobCheckpoint job = checkpoints.job(jobId);
            return job == null || job.acceptedBy() == id;
        }

        private void write(java.util.function.Supplier<CheckpointEntry> create, boolean waitForPeer) {
            CheckpointEntry entry;
            List<CheckpointEntry> snapshot;
            synchronized (checkpoints) {
                entry = create.get();
                if (entry == null) return;
                checkpoints.apply(entry);
                snapshot = checkpoints.snapshot(entry.getJobId());
            }
            CheckpointReplicator active = replicator;
            if (active == null) {
                if (waitForPeer) throw io.grpc.Status.UNAVAILABLE
                        .withDescription("Checkpoint replication is not ready").asRuntimeException();
                return;
            }
            // Send cumulative snapshots so newer updates remain complete if RPCs arrive out of order.
            boolean confirmed = active.replicate(snapshot, entry.getTerm(), waitForPeer);
            if (waitForPeer && !confirmed) throw io.grpc.Status.UNAVAILABLE
                    .withDescription("Job acceptance is unconfirmed: retry the same job ID when a replica is available")
                    .asRuntimeException();
            recordEvent(WorkerEvent.CHECKPOINT_REPLICATED, "job " + entry.getJobId() + " sequence "
                    + entry.getSequence() + " (" + entry.getEntryCase() + ")");
        }
    }

    @Override
    public void ping() {
    }

    // Stops both pools; daemon threads mean a missed call cannot block JVM exit.
    public void shutdown() {
        if (grpcJobs != null) grpcJobs.close();
        if (quorumMonitor != null) quorumMonitor.close();
        if (quorumProbe != null) quorumProbe.close();
        if (grpcDirectory != null) grpcDirectory.close();
        if (bootstrapLease != null) bootstrapLease.shutdownNow();
        if (healthMonitor != null) healthMonitor.close();
        if (probe != null) probe.close();
        if (replicator != null) replicator.close();
        ElectionTransport transport = electionTransport;
        if (transport != null) transport.close();
        if (progressServer != null) progressServer.shutdownNow();
        executor.shutdownNow();
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

}
