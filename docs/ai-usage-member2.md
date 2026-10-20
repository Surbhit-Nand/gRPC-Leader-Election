# AI tools usage log - Member 2 (leader election and coordinator failover)

> Historical implementation log. Paths and compatibility descriptions below
> predate the [gRPC-only module migration](grpc-module-migration.md).

A running record of what was generated or changed with AI assistance, per file, so the
report's AI-tools page is accurate. Written by hand from the git history of
`feature/leader-election-grpc` and `feature/coordinator-failover`.

**How this work was produced.** An AI coding assistant (OpenAI's Codex, running as an opencode
agent) worked directly in the repository: it read the existing code, wrote the new classes,
edited the shared files, ran the Maven build and test suite, and wrote the documentation. The
work was reviewed, steered and corrected by the member throughout - every judgement call in
`docs/failover.md` section 7 was made by the member, not by the tool. No code was accepted
without a green build; three of the bugs listed below were found by the AI's own tests.

**Prompting approach.** One long, detailed work order per work item (leader election first,
coordinator failover second), each specifying the frozen-contract rules, the team boundaries, the
required tests and the priority order. The tool was asked to stop and ask only when blocked,
and to take the less invasive option where two were reasonable.

---

## Files created (all new, no conflict with other members)

| File | What it is |
| --- | --- |
| `src/main/proto/failover.proto` | New contract: `Heartbeat`, `Replicate`, `FetchCheckpoints` and the checkpoint entry kinds. Deliberately a new file so no frozen proto was renumbered. |
| `src/main/java/com/cs324/election/ElectionTransport.java` | Transport seam for one election hop. |
| `src/main/java/com/cs324/election/GrpcElectionTransport.java` | gRPC hops: registry-resolved endpoints, one cached channel per peer, per-call deadlines. |
| `src/main/java/com/cs324/election/RmiElectionTransport.java` | Removed in the 20 October gRPC-only migration; no RMI fallback remains. |
| `src/main/java/com/cs324/election/ElectionHandler.java` | The worker's election entry points, in the shape the service needs. |
| `src/main/java/com/cs324/election/ElectionGrpcService.java` | Server side of `ElectionService`. |
| `src/main/java/com/cs324/failover/CheckpointLog.java` | The replica: folds entries per job by sequence and term. |
| `src/main/java/com/cs324/failover/JobCheckpoint.java` | One job's replicated state: request, slices with inputs, owners, attempts, results. |
| `src/main/java/com/cs324/failover/CheckpointReplicator.java` | Pushes entries to every peer, reads peers back during takeover. |
| `src/main/java/com/cs324/failover/CheckpointSource.java` | Peer-discovery interface, so takeover decisions are unit-testable. |
| `src/main/java/com/cs324/failover/CheckpointSink.java` | The transitions the job service publishes to replication. |
| `src/main/java/com/cs324/failover/CoordinatorHealth.java` | The alive/suspect/failed rule, with no I/O in it. |
| `src/main/java/com/cs324/failover/CoordinatorHealthMonitor.java` | Drives that rule over gRPC; the cluster's only failure detector. |
| `src/main/java/com/cs324/failover/GrpcCoordinatorProbe.java` | Sends one heartbeat to one coordinator. |
| `src/main/java/com/cs324/failover/FailoverCoordinator.java` | Merge, decide per job, rebuild. |
| `src/main/java/com/cs324/failover/FailoverHandler.java` | Worker-side behaviour the failover service needs. |
| `src/main/java/com/cs324/failover/FailoverGrpcService.java` | Server side of `FailoverService`. |
| `src/main/java/com/cs324/failover/RestoredJob.java` | A replicated job handed back to the job service. |
| `src/test/java/com/cs324/election/GrpcElectionTest.java` | Election over real TCP gRPC; the legacy transport was removed. |
| `src/test/java/com/cs324/failover/CoordinatorFailoverTest.java` | End-to-end coordinator failure and recovery. |
| `src/test/java/com/cs324/failover/CoordinatorHealthTest.java` | The liveness rule, including the demo timing window. |
| `src/test/java/com/cs324/failover/CheckpointLogTest.java` | Folding: duplicates, stale entries, deposed writers. |
| `src/test/java/com/cs324/failover/FailoverCoordinatorTest.java` | Takeover decisions against scripted peer states. |
| `docs/failover.md` | Report raw material (Q1, Q3, Q4, Q5, decisions, gaps). |

## Files created with AI assistance, reviewed line by line

All of the above. Nothing was merged without being read; the constants, the winner rule and the
"do not steal a live job" rule were specified by the member and only implemented by the tool.

---

## Files owned by other members that were edited

Small and additive in every case. **These are the edits the other members should review.**

### `src/main/java/com/cs324/computation/GrpcJobService.java` (Member 3)

Additive only; no restructuring, no renamed methods, no changed behaviour on the normal path.

1. `setCheckpointSink(...)` plus a private `checkpoint(...)` wrapper - the replication seam.
2. Sink calls at the transitions that already existed: `Run.begin` (accepted), `dispatch`
   (slice), `recover` (reassigned), `complete` (result), `finish` (tombstone), and the client
   delivery observer (delivered / delivery failed).
3. `restore(RestoredJob, remainingMillis)` - re-registers a replicated job, deliberately outside
   the admission predicate, and claims the job id in `submissions`.
4. `deliverResult(SubmitJobRequest, TaskResult)` - replays a computed answer for a tombstone.
5. `coordinatedJobIds()` - observability accessor.
6. A private `Run(RestoredJob)` constructor and `Run.beginRestored(...)`.
7. **Bug fix (trap 1):** `assignTask` now accepts the same `(job, task, attempt)` from a
   *different* coordinator, updates where the answer is reported, and re-sends the cached
   result, instead of answering `ALREADY_EXISTS` and making the sender recompute finished work.
8. **Bug fix:** `sendAssignment` rebuilds the cached `TaskAssignment` when the reporting
   coordinator changed, not only when the attempt changed - otherwise results from the same
   attempt keep going to the dead leader after a failover.
9. `Work` changed from a record to a small class so the coordinator endpoint can be updated;
   the `request()`/`result()` accessors were kept so the rest of the file is untouched.

### `src/main/proto/election.proto` (Member 1, frozen contract)

Additive fields only, as permitted for this work item:

| Message | Field | Number |
| --- | --- | --- |
| `ElectionMessage` | `int64 term` | 5 |
| `EchoMessage` | `int64 term` | 5 |
| `CoordinatorAnnouncement` | `int64 term` | 3 |

No existing field was renumbered, removed or retyped, and no RPC was added or renamed.
`ProtoContractsTest` still passes unchanged.

### `src/main/java/com/cs324/common/Constants.java` (shared)

Added the `FAILOVER_*` block. The historical `LEGACY_RMI_HEARTBEAT_ENABLED` flag was
removed by the gRPC-only migration. No existing constant changed
value. Another member is moving configuration to files, so these belong together with theirs
when that happens.

### `src/main/java/com/cs324/common/WorkerEvent.java` (shared)

Added eight event-kind constants: `COORDINATOR_SUSPECTED`, `COORDINATOR_FAILED`,
`FAILOVER_STARTED`, `FAILOVER_COMPLETED`, `JOB_RECOVERED`, `CHECKPOINT_REPLICATED`,
`LEADERSHIP_STEP_DOWN`. No GUI file was touched.

### `src/main/java/com/cs324/worker/WorkerRemoteImpl.java` (shared)

The election wiring (transport seam, term handling) and the failover wiring (replica, replicator,
health monitor, takeover trigger, shutdown). Diff is additive: new fields, new private methods
and small delegating changes inside the existing election methods, which keep their signatures.

### `docs/election.md`, `README.md` (shared)

Documentation updates: the election document now describes the gRPC transport, and the README
gains the demo script plus a correction to the paragraph that said a replacement coordinator
could not restore the previous coordinator's jobs.

---

## Bugs the AI's tests found (and the member fixed)

Worth stating because they are the kind of thing an unreviewed generated change would have
shipped:

1. `getProgressPort()` threw `IllegalStateException: Already terminated` once a worker's gRPC
   server was down, so a dispatch racing a shutdown failed the job and delivered a spurious
   error to the client. The bound port is now cached.
2. Replication stopped at the first peer that acknowledged, leaving the other replicas unable to
   fold any later delta (a delta carries no input, so they would have been unable to take that
   job over). It now writes to every peer and waits for the first acknowledgement.
3. A checkpoint snapshot named the *serving replica* as the job's coordinator, so every
   replicated job looked abandoned and a live owner's job could have been stolen.
4. The failover initially trusted the directory as proof of life; a crashed worker stays
   registered until it exits, so recovery has to learn liveness from the takeover fetch.

---

## Commands used

```powershell
git fetch --all; git branch -a
mvn test                                   # baseline: 178 tests green on origin/main
mvn -o compile
mvn -o test "-Dtest=CoordinatorFailoverTest,GrpcElectionTest"
mvn -o test                               # final: 216 tests green
```

## Honest limits of this assistance

- The failover design follows the member's direction; the tool implemented it and wrote the
  tests, including the tests that found the bugs above.
- Nothing was deployed, benchmarked on real hardware, or run as a scripted chaos test: only
  single-node-in-one-JVM integration tests, plus a manual demo procedure.
- The verification of the manual demo path (killing a terminal during a live job) is the
  member's responsibility; the automated test covers the same scenario in-process.
