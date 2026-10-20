# Coordinator failure and checkpoint recovery

The worker module implements failover in `com.cs324.failover`. Every worker serves
the `FailoverService` contract from `shared/src/main/proto/failover.proto`.

## Detection and leadership

`CoordinatorHealthMonitor` uses bounded gRPC heartbeats. A slow successful reply
marks a coordinator suspect; repeated missed replies or prolonged silence mark it
failed. `FailureGate` suppresses repeated election requests for the same failure.
The normal gRPC election selects a replacement and requires a strict majority of
ready workers before announcing it. Leadership terms stop older announcements from
replacing a newer view. A coordinator also renews a majority lease with bounded
`WorkerControlService.GetStatus` probes. Two consecutive failed checks, or a stale
lease at admission time, make it refuse new jobs and step down.

When a worker becomes coordinator, it temporarily refuses new jobs, fetches
reachable checkpoint snapshots, merges them, and restores work whose previous
owner failed. Existing jobs owned by live coordinators are not stolen.

## Acceptance requires replication

The acceptance snapshot contains the original request, client endpoint,
acceptance time, slice identities, inputs, owners and attempts. The coordinator
sends it to every published peer and waits up to
`Constants.FAILOVER_REPLICATION_DEADLINE_MS` for one acknowledgement covering
the snapshot's highest sequence.

No peer, an RPC failure, interruption, timeout or stale acknowledgement does
**not** count as confirmation. The coordinator returns retryable `UNAVAILABLE`
without starting computation and releases the reserved job-budget slot.
At least two live workers are therefore required to accept new work.

A lost acknowledgement makes the outcome uncertain: a peer may have stored the
snapshot even though the caller received an error. Retrying the same job ID and
payload is required. Checkpoints for uncertain submissions can be recovered after
failure; this is ordinary RPC ambiguity, not proof that the job was never received.

## State and takeover

After acceptance, slice assignment, reassignment, completion, final result and
delivery acknowledgement update the checkpoint log. Every outbound push carries a
cumulative snapshot, so reordered asynchronous RPCs cannot discard earlier
information. Sequence allocation, local folding and snapshot capture are atomic.

Takeover merges detached views of reachable replicas, preserving completed slices
and attempts. Unfinished work resumes with the original deadline, subject to the
configured minimum recovery window. A restored job whose slices are all complete
aggregates immediately. A finished checkpoint replays the saved aggregate, not a
single slice. Delivery is recorded only after the callback acknowledges it.

Snapshots include retained finished/delivered markers so a stale replica does not
resurrect a job another survivor knows is complete. Terminal markers do not invent
new sequence numbers. Finished state has bounded retention.

## Guarantees and limits

- A successful submission acknowledgement means at least one other worker
  confirmed the acceptance checkpoint.
- Recovery needs a surviving replica and a reachable replacement coordinator.
- Storage is in memory. Losing all replicas, or restarting the whole cluster,
  loses their checkpoints.
- Leadership requires a strict majority of ready workers. Election and ongoing
  leadership use a lease, so an isolated minority cannot elect or keep serving.
- A two-of-four remnant is therefore safely unavailable rather than split-brained;
  it resumes when enough workers return.
- This is not a durable consensus log (Raft/Paxos). Leadership safety comes from
  the majority lease, but in-memory checkpoints and replication remain best-effort.
- Later progress/completion replication is asynchronous. Unreplicated completed
  work may be recomputed; task identities prevent stale results being added twice
  to one restored job.
- Callback acknowledgements can be lost. The client must deduplicate by job ID;
  transport delivery is not globally exactly once.
- Bootstrap itself is a single in-memory directory and has no failover replica.
  Membership entries expire after a 15-second lease, and its ID counters are
  persisted in `bootstrap-state.properties` so a restart does not immediately
  reuse an identity. Active membership is still not persisted.

## Verification and demo

```text
mvn "-Dtest=CoordinatorFailoverTest,GrpcOnlyClusterTest,CheckpointLogTest,CheckpointReplicationTest,ReplicationAdmissionTest,GrpcControlRegressionTest,GrpcElectionTest,QuorumMonitorTest" test
```

`GrpcOnlyClusterTest` starts actual worker JVMs and forcibly terminates a
coordinator during PRIMESUM, then checks election, the recovered answer and
completed progress. Other tests cover reordered checkpoints, saved-result replay,
delivery acknowledgement, stale replication acknowledgements, failed admission,
minority partitions and quorum lease expiry.

For a manual demonstration, follow the root README with four workers. Submit a
long prime range and terminate the coordinator while work is active. Observe the
replacement leader, restored job and progress stream reconnecting. Keep a majority
(three of four) if you also want to submit new jobs after the failure.
