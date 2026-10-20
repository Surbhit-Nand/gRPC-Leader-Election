# Election over gRPC

The implementation is in
`worker/src/main/java/com/cs324/worker/WorkerRuntime.java` and
`worker/src/main/java/com/cs324/election/`.
The contract is `shared/src/main/proto/election.proto`.

`ElectionService` carries `SendElection`, `SendEcho` and
`AnnounceCoordinator`. `GrpcElectionTransport` resolves published `grpc_port`
endpoints through `RegistryService`, caches channels and applies per-call
deadlines. There is no alternate election transport.

## Protocol

1. A root creates a wave ID from its worker ID and local counter, votes for
   itself, and forwards to its neighbours.
2. A receiver merges its own candidacy: lower JAC wins, then higher worker ID.
   It forwards to neighbours other than its parent.
3. Repeated waves receive an echo containing the best candidate already known.
   They are not silently dropped, which would leave a sender waiting in a cycle.
4. Echoes update the best candidate and pending-child set under a lock.
   Each wave echoes once when no children remain. Echoes also carry the set of
   worker IDs that participated, so the root can count the completed wave.
5. The root announces the winner only when recognised voters form a strict
   majority of ready workers. Otherwise it records `ELECTION_NO_QUORUM`, sends no
   announcement, and steps down if it was the current leader.
6. Announcements carry a term above the terms observed by the wave. A worker
   accepts a newer `(term, coordinator ID)` view. Termless or stale announcements
   cannot replace leadership. Duplicate announcements do not circulate.

## Quorum lease

`QuorumMonitor` runs only while this worker is coordinator. Every
`QUORUM_CHECK_INTERVAL_MS` it takes the ready worker IDs and probes peer
`WorkerControlService.GetStatus` with `QUORUM_PROBE_DEADLINE_MS`. Self and each
reachable member count as a vote; the threshold is `floor(n/2) + 1`. Membership
read failures count as loss.

A successful check renews a `QUORUM_LEASE_MS` lease. Two consecutive failed checks
clear the coordinator view, record `QUORUM_LOST` and `LEADERSHIP_STEP_DOWN`, and
prevent new admissions. `reserveTermSlot` also requires a fresh lease, so a
coordinator cannot accept work while its majority is stale. Election voters are
intersected with current ready membership, so departed or unregistered workers
cannot manufacture a majority.

A failed hop removes the unreachable edge. Repair first tries remembered
two-hop neighbours, then other discovered workers. Peer connections and graceful
leave notifications use `WorkerControlService`.

## Jobs and leadership

JAC counts assignments to other workers. A separate counter limits a coordinator
to five accepted client jobs per term. Duplicate submissions do not consume new
slots. Failed acceptance releases a reserved slot. The fifth acceptance triggers
rotation; the former leader refuses further submissions during handover.

Joining workers adopt a known leader and term locally without broadcasting a new
announcement. Startup triggers an election only when no coordinator is known.

## Verification

```text
mvn "-Dtest=GrpcElectionTest,GrpcControlRegressionTest,QuorumMonitorTest" test
```

These tests use TCP gRPC for chain/triangle elections, winner selection, duplicate
echoes, dead peers, stale announcements, five-job rotation, topology repair,
quorum-lease renewal/expiry, and leader step-down. Partition tests verify that two
isolated minority groups elect no coordinator and reconverge after healing. No RMI
registry is created by the tests.
