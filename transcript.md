# Part 2 — Leader election and duplicate handling

Updated for the gRPC-only module layout, 20 October 2026.
Open `worker/src/main/java/com/cs324/worker/WorkerRemoteImpl.java`.
Use method names when navigating: line numbers change as the implementation evolves.
This section takes about five minutes.

## Start with the architecture

**Show:** the root `pom.xml`, then the `bootstrap`, `worker` and `client` folders.

> “These are separate Maven application projects. They share the protobuf contracts
> and common helpers in `shared`. Bootstrap allocates IDs and publishes worker
> endpoints. Workers run the election and computation. The client submits jobs,
> receives callbacks and watches progress. All network calls use gRPC.”

**Show:** `shared/src/main/proto/election.proto`.

> “The election service has three calls: send a wave, send an echo, and announce
> the coordinator. Every message carries its election data as protobuf fields.
> There is no RMI registry or fallback election transport.”

## Starting an election

**Show:** `startElection()`.

> “The worker builds a unique wave ID from its own ID and a local counter. It
> starts with itself as the best candidate, remembers its neighbours as pending
> children, and sends the wave to each neighbour through `GrpcElectionTransport`.
> If no reachable neighbour remains, the wave can finish immediately.”

**Show:** `GrpcElectionTransport.stubFor()` and its deadline configuration.

> “The transport resolves the published gRPC endpoint through bootstrap. Calls
> have deadlines, so a failed peer cannot leave one network call waiting forever.
> An unsuccessful hop removes that peer from the pending work and triggers repair.”

## Choosing the candidate

**Show:** `applyElectionMessage()` and `applyEchoMessage()`.

> “The winner has the lowest job-allocation counter. If the counters are equal,
> the higher worker ID wins. Both forwarding and echo merging use that rule.
> JAC counts assignments to other workers; the five-client-job leadership budget
> is a separate counter.”

## Duplicate waves must receive an answer

**Show:** `elections.compute(...)` and the `alreadySeen` branch in
`applyElectionMessage()`.

> “The map claims each wave atomically, so concurrent receivers cannot create
> two independent states for one wave. If we have seen it before, we read the
> best candidate already known and send an echo back. We do not forward it again.”

> “We must answer rather than silently drop the duplicate. In a triangle, a wave
> can arrive through two paths. A sender retires its pending child only when it
> gets an echo or a failed hop. Silence would leave the wave waiting.”

## Echoes finish a wave once

**Show:** `applyEchoMessage()` and `propagateEcho()`.

> “Each echo merges the child's candidate and removes the child from the pending
> set under the wave's lock. When the set is empty, the echoed flag ensures one
> completion. A non-root worker echoes upward; the root announces the winner.”

## Announcements are different from waves

**Show:** `applyCoordinatorAnnouncement()` and `acceptLeadership()`.

> “Repeated announcements are dropped without forwarding. Otherwise an
> announcement could circulate around a cycle forever. They have their own
> deduplication set, separate from the wave-state map.”

> “An announcement also carries a leadership term. A worker accepts a newer
> term-and-coordinator view and rejects stale or termless announcements. Receiving
> the same leadership view again cannot reset the serving coordinator's budget.”

## Rotation and failure

**Show:** `reserveTermSlot()`, `releaseTermSlot()` and `maybeRotateCoordinator()`.

> “A coordinator reserves at most five distinct jobs in one term. Duplicate
> submissions reuse the existing job. Failed acceptance releases its slot.
> The fifth accepted job starts a new election, and the old coordinator refuses
> further jobs during handover.”

**Show:** `ReplicatingCheckpointSink` and `CheckpointReplicator.replicate()`.

> “Before a submission is acknowledged, another worker must confirm its acceptance
> snapshot. Without a confirmed replica, the coordinator returns a retryable error
> and starts no computation. That means a demonstration needs at least two workers.”

> “After a coordinator crash, surviving workers elect a replacement and merge
> checkpoint snapshots. They preserve known completed slices and resume unfinished
> work. Replication is in memory, so this depends on a replica surviving; it is not
> a disk-backed quorum system.”

## Evidence to show

Open these files in `integration-tests/src/test/java/com/cs324/`:

- `election/GrpcElectionTest.java`: chain/triangle convergence, winner selection,
  duplicate echoes and dead peers, using real gRPC calls.
- `worker/GrpcControlRegressionTest.java`: duplicate submissions, concurrent
  five-job limits, stale announcements, graceful leave and crash repair.
- `failover/ReplicationAdmissionTest.java`: failed, stale and valid replication
  acknowledgements.
- `worker/GrpcOnlyClusterTest.java`: actual worker JVMs, coordinator termination,
  recovered answer and completed progress.

Run the focused election/control checks from the repository root:

```text
mvn "-Dtest=GrpcElectionTest,GrpcControlRegressionTest" test
```

For setup and a full failure demonstration, follow [README.md](README.md).
