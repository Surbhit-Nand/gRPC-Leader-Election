# Coordinator failover (Member 2)

What happens when the worker that is currently coordinating closes, crashes or stops
answering: the survivors notice, elect a replacement, rebuild the work the dead
coordinator was holding, and the client still gets one correct answer.

This document is the raw material for the report questions, not the report itself:
(Q1) the contract and why each RPC exists, (Q3) how failure is detected, (Q4) what state
must survive, (Q5) the alternatives. Section 7 lists every judgement call and section 8 the
known gaps, stated plainly.

Code: `src/main/java/com/cs324/failover/`, `src/main/proto/failover.proto`, and the wiring in
`src/main/java/com/cs324/worker/WorkerRemoteImpl.java`.

---

## 1. The contract (Q1)

`failover.proto` is a **new file**, so nothing in the frozen `registry.proto`,
`election.proto`, `job.proto` or `progress.proto` was renumbered or removed. Three RPCs, all
hosted by every worker.

### 1.1 `Heartbeat`

```proto
rpc Heartbeat(HeartbeatRequest) returns (HeartbeatReply);

message HeartbeatRequest { int32 worker_id = 1; int64 term = 2; int32 coordinator_id = 3; }
message HeartbeatReply     { int64 term = 1; int32 coordinator_id = 2; bool is_coordinator = 3; }
```

**Why a heartbeat RPC at all**, when the slice probes already exist: a probe answers "is that
slice done", and a missing answer is ambiguous - it can mean a crashed worker, a slow worker,
or a lost result callback, and Member 3's recovery deliberately treats all three the same way.
A liveness question needs an answer that is about the *coordinator as a process*, and it has to
be cheap enough to ask every second from every worker.

**Why the reply carries the responder's view.** This is the part that earns its place. A
timeout cannot distinguish slow from dead, and it also cannot detect the failure nobody is
waiting for: a coordinator that is up, answering, and no longer the leader. The reply's
`(term, coordinator_id, is_coordinator)` lets a follower do three things it otherwise cannot:

1. see that the peer it probes is *not* the coordinator, and re-discover rather than elect;
2. notice a **higher term**, which means the cluster has already moved on, and adopt that view
   instead of starting an election it does not need;
3. let a **deposed coordinator** discover it has been replaced: it receives a higher term from
   a peer, steps down, and stops admitting jobs.

Because the same RPC works against *any* worker, re-discovery needs no extra call: a follower
that doubts its leader asks its neighbours, using the same RPC, and takes the highest
`(term, coordinator_id)` view it hears.

### 1.2 `Replicate`

```proto
rpc Replicate(ReplicateRequest) returns (ReplicateAck);
message ReplicateRequest { int32 coordinator_id = 1; int64 term = 2; repeated CheckpointEntry entries = 3; }
message ReplicateAck      { int64 acknowledged_sequence = 1; }
```

**Why.** A coordinator that accepts a job and then dies takes the job with it. If the only copy
of "job 7 was accepted, split into four slices like this" lives in the dead process, the cluster
cannot continue job 7, and the client either waits forever or resubmits into a cluster that may
run the work twice. So acceptance is written to the peers *before* the client is acknowledged.

**Why entries and not a snapshot.** One message per transition keeps the write proportional to
what changed: an acceptance is the expensive message and happens once per job, while dispatch,
reassignment and completion are small and frequent. It also means a peer's log is a history a
human can read, which matters when a demo goes wrong.

**Why `acknowledged_sequence`.** It tells the sender the highest per-job sequence the replica
holds, so a sender that lost an acknowledgement can resend only what is genuinely missing. It is
an optimisation, not a correctness mechanism: the sequence rules in section 4 make a duplicate
harmless either way.

### 1.3 `FetchCheckpoints`

```proto
rpc FetchCheckpoints(FetchRequest) returns (FetchReply);
message FetchRequest { int32 requester_id = 1; }
message FetchReply   { int32 replica_id = 1; repeated CheckpointEntry entries = 2; }
```

**Why.** Takeover is a read. Rather than require survivors to have heard *every* transition in
real time, the new coordinator asks each reachable peer for the state it holds and merges. That
makes the design robust to exactly the case that matters: a worker that was partitioned, busy,
or briefly unreachable while the coordinator was alive.

**Why a snapshot per job rather than a raw log.** `FetchReply` carries, per recoverable job,
one `JobAccepted` holding the current slice table (including any results already produced) and
a `JobFinished` tombstone if it had finished. Two messages per job instead of an unbounded
history, and the receiving side still folds through the same idempotent rules.

### 1.4 Entry kinds

| Kind | Carries | Why |
| --- | --- | --- |
| `JobAccepted` | the original `SubmitJobRequest`, every slice with its `TaskPayload`, owner and attempt, and the acceptance time | the request lets a resubmitted job attach instead of rerunning; the payloads let a survivor rebuild the *same* split; the time keeps the original deadline |
| `SliceUpdated` | `task_id`, optional new `owner`, optional new `attempt`, optional `result` | one small delta per transition; absent fields mean unchanged, so an update can be "reassigned", "finished", or both |
| `JobFinished` | the final result and whether the client callback was acknowledged | a finished job must never be rebuilt; an unacknowledged callback may still need replaying |

The `SubmitJobRequest`, `TaskPayload`, `TaskResult` and `WorkerEndpoint` types are imported from
the frozen files rather than redefined, and no payload is an opaque `bytes` blob - every field
is readable in a log line or a test assertion.

---

## 2. Failure detection (Q3)

`CoordinatorHealth` holds the rule with no I/O in it; `CoordinatorHealthMonitor` drives it and
is the cluster's **only** failure detector. The legacy RMI heartbeat tick is retained but
disabled behind `Constants.LEGACY_RMI_HEARTBEAT_ENABLED = false`, because two detectors racing
to start elections is how a cluster ends up with one wave per survivor.

### 2.1 The states

| State | Meaning | What it may cause |
| --- | --- | --- |
| `ALIVE` | the last probe answered inside the slow threshold | nothing |
| `SUSPECT` | answered but slowly, or one probe missed | a feed line, nothing else |
| `FAILED` | `FAILOVER_HEARTBEAT_FAILURE_THRESHOLD` consecutive misses, **or** no success within the silence window | one election, on the transition only |

**Active, slow and failed are three different things**, and the middle one is the interesting
case. A coordinator that answers in 900 ms is not dead: electing a replacement for it would cost
an election and, worse, split the cluster's belief about who owns running jobs. So a slow answer
moves it to `SUSPECT` and no further; a *later* fast answer returns it to `ALIVE` with no
anything happening. Only repeated silence, or silence long enough that an earlier success cannot
vouch for the present, is `FAILED`.

**Cannot resolve or cannot connect counts as a miss.** This is the case the old RMI tick got
wrong: when `resolveCoordinatorStub` failed it returned silently, so a coordinator whose address
vanished from the directory could go unnoticed indefinitely. `GrpcCoordinatorProbe` treats a
failed directory lookup, a failed connect and a deadline overrun as one thing - a probe that did
not complete.

### 2.2 Constants

All in `com.cs324.common.Constants`; no magic numbers.

| Constant | Value | Role |
| --- | --- | --- |
| `FAILOVER_HEARTBEAT_INTERVAL_MS` | 1000 | how often a worker probes the coordinator it believes in |
| `FAILOVER_HEARTBEAT_DEADLINE_MS` | 600 | per-call deadline, well under the interval so a slow coordinator shows up as a miss instead of overlapping probes |
| `FAILOVER_HEARTBEAT_SLOW_MS` | 400 | a reply slower than this is `SUSPECT` rather than `ALIVE` |
| `FAILOVER_HEARTBEAT_FAILURE_THRESHOLD` | 3 | consecutive misses before `FAILED` |
| `FAILOVER_HEARTBEAT_SILENCE_MS` | 8000 | no success at all for this long is failure regardless of the streak |
| `FAILOVER_ELECTION_JITTER_MS` | 400 | random delay before an automatic failover wave |
| `FAILOVER_SUPPRESSION_MS` | 3000 | two failures of the same coordinator inside this window are one failure |
| `FAILOVER_REPLICATION_DEADLINE_MS` | 1000 | how long acceptance waits for a peer's acknowledgement |
| `FAILOVER_REPLICATION_RPC_DEADLINE_MS` | 700 | per-call deadline for slice/finish pushes |
| `FAILOVER_FETCH_DEADLINE_MS` | 1500 | per-call deadline for the takeover fetch |
| `FAILOVER_CHECKPOINT_RETENTION` | 256 | finished jobs dropped from a replica past this count |
| `FAILOVER_MIN_REMAINING_MS` | 30000 | floor for a restored job's deadline |
| `LEGACY_RMI_HEARTBEAT_ENABLED` | false | the old RMI detector, kept for reference only |

**Detection time.** A miss costs at most interval + deadline = 1600 ms, and three are needed,
so a dead coordinator is declared failed in **3.0-4.8 s**, plus scheduling and (if the JAC rule
points elsewhere) a fresh election. `CoordinatorHealthTest.defaultsDetectADeadCoordinatorInRoughlyThreeToFiveSeconds`
asserts this arithmetic stays inside the documented window rather than drifting silently.

### 2.3 One detector, one election

The monitor fires `onFailed` only on the transition into `FAILED`. The reaction is
`requestFailoverElection`, which:

1. **suppresses duplicates** through `FailureGate` - the same coordinator failing again inside
   `FAILOVER_SUPPRESSION_MS` is ignored, so N survivors noticing the same death produce one
   wave rather than N. The gate is keyed on (coordinator, when), not on the coordinator alone:
   keying on the id would suppress a *second, unrelated* death of the same worker id for ever,
   leaving the cluster with a dead coordinator and no election to replace it;
2. **jitters** by up to `FAILOVER_ELECTION_JITTER_MS` before starting, so the survivors' waves do
   not leave the cluster at exactly the same instant;
3. then calls the ordinary `startElection()`, so the election itself is unchanged code.

A manual election (`[1]` in a worker's console, or the startup election) deliberately bypasses
this path: a person asking for an election always gets one.

### 2.4 Terms and deposed coordinators

Each announcement carries a **term**, and the root announces `max(known term) + 1`
(`docs/election.md`). A worker applies a term-bearing announcement only when
`(term, coordinator_id)` is not older than the pair it holds, ties going to the higher tuple.

The heartbeat reply lets the same rule be applied to what a *peer reports* about leadership, and
`LeaderView.decide` is that rule in one testable place:

| The responder reports | Action | Why |
| --- | --- | --- |
| a newer `term` | adopt its view | a newer election has already concluded |
| the same term, but `is_coordinator=false` naming someone else | adopt its view | the worker we thought was leading has already stood down |
| an older `term` | ignore | a deposed coordinator must not pull the cluster back |
| the same coordinator we already hold | ignore | agreement, not a contradiction |

So a coordinator that comes back after being replaced learns the higher term from a peer, adopts
the new leader (`LEADERSHIP_STEP_DOWN`) and stops admitting jobs, instead of reclaiming a term it
has lost. A worker that receives an older announcement drops it, so a late wave cannot undo a
newer election.

---

## 3. What survives a coordinator failure (Q4)

| State that must survive | How it is preserved | Where it lives |
| --- | --- | --- |
| **A job was accepted**, with its job id, type and payload | `JobAccepted` replicates the original `SubmitJobRequest`, and acceptance waits (up to `FAILOVER_REPLICATION_DEADLINE_MS`) for at least one peer to acknowledge before the client is acknowledged | peers' `CheckpointLog` |
| **The split**: how many slices, which input each holds | every slice's `TaskPayload` travels in `JobAccepted`, so the survivor rebuilds the *same* split instead of re-deriving one | `JobCheckpoint.Slice.input` |
| **Who owns which slice, and at which attempt** | `SliceUpdated` on dispatch and on reassignment; the fetch snapshot carries the current owner and attempt | `JobCheckpoint.Slice.owner/attempt` |
| **Work already finished** | a completed slice's `TaskResult` is replicated, and the fetch snapshot includes it; the restored job is seeded with those results so they are **never recomputed** - this is what stops a PRIMESUM or PRIMECOUNT being added twice | `JobCheckpoint.Slice.result` |
| **The client endpoint**, so the answer reaches the same client | inside the replicated `SubmitJobRequest` | replicated request |
| **The job's original deadline** | `accepted_at_millis` plus `FAILOVER_MIN_REMAINING_MS` as a floor, so a restored job is not silently given a fresh full timeout | `JobAccepted.accepted_at_millis` |
| **That an answer was already delivered** | `JobFinished.delivered`; a survivor replays a result only when delivery was never confirmed | `JobCheckpoint.delivered` |
| **Which coordinator owned each job** | `CheckpointEntry.coordinator_id`, preserved through snapshots; a new leader skips a job whose owner is still alive | `JobCheckpoint.acceptedBy` |
| **Ordering** of all of the above | per-job monotonic sequence plus the leadership term; an entry is applied only if its sequence is newer and its term is not older | `CheckpointLog.apply` |

### 3.1 Why a resumed sequence space works

A new coordinator continues the sequence it merged: the highest sequence any survivor holds is
in its log, and its first write is `max + 1`. That is what makes "highest sequence per job wins"
comparable across coordinators rather than only within one coordinator's lifetime. It also makes
a deposed coordinator harmless: its writes carry the old term and lower sequences, so replicas
reject them.

### 3.2 Takeover

Triggered when this worker becomes coordinator, and idempotent per `(job, term)`:

1. **Merge.** Ask every reachable peer for its replica; fold everything through the sequence
   and term rules. A peer that does not answer is recorded as gone - a crashed worker stays in
   the directory until it exits, so "listed" is not "alive".
2. **Decide per job.**
   - finished + delivered -> skip;
   - finished + not delivered -> replay the answer to the client, once;
   - owner still alive -> **skip**. Job ownership is not leadership: after an ordinary five-job
     rotation the previous coordinator is alive and still finishing its work, and stealing that
     job would run it twice;
   - owner gone -> rebuild.
3. **Rebuild per slice.**
   - has a result -> keep it, do not recompute;
   - owner alive -> keep owner and attempt, and re-probe it against the *new* coordinator
     endpoint;
   - owner gone -> give it to the new coordinator with `attempt + 1`, which also fences any
     late answer from the previous owner.
4. **Restore** through `GrpcJobService.restore(RestoredJob, remainingMillis)`: it registers the
   job and its original request in the idempotency map, republishes progress and restarts the
   probe and deadline timers. Because the request is registered, **a client that resubmits the
   same job id after the failure attaches to the recovered job** instead of starting a second
   run of the same work.
5. **Refuse new work while merging** (`failoverInProgress`), so the same job cannot be accepted
   fresh in one window and resumed from the log in another.

**Recovered jobs do not consume the new term's five-job budget.** They were already accepted and
billed to a client under the previous coordinator; charging them again would either refuse work
the cluster already owes a client, or make a term shorter than the spec says. `restore` therefore
bypasses the admission predicate, deliberately and only for this path.

---

## 4. Folding rules

One rule governs every write, and it is why the design tolerates duplicates, reordering and a
deposed writer:

```
apply(entry) only if   entry.term >= job.term   and   entry.sequence > job.sequence
```

A delta or a tombstone for a job this replica never saw accepted is dropped: it carries no input,
so nothing could be rebuilt from it anyway. Retention drops the oldest finished jobs past
`FAILOVER_CHECKPOINT_RETENTION`, which bounds replica memory.

---

## 5. Alternatives considered and rejected (Q5)

**A single copy of the job log on the Bootstrap node.** Simplest to reason about, and one round
trip instead of N. Rejected: it replaces the coordinator's single point of failure with the
directory's. The Bootstrap node is a peer-links and job-id service that every other member owns,
and making it also hold coordinator state means a Bootstrap restart loses every running job, and
a Bootstrap bug becomes a cluster-wide data-loss bug. It also fails the assignment's "all
communication between distributed components must use gRPC" in spirit by concentrating state in
the one process that is not a participant.

**Disk checkpoints (a local file per worker).** Rejected because it does not survive the case
that matters: the machine is gone. In this assignment all nodes run on one machine, so a file
would appear to work while testing and lose everything when a machine is lost or a container is
rescheduled. It also adds a durability story (fsync, partial writes, compaction) that the
assignment does not ask for, in exchange for surviving a failure the assignment does not ask
about. In-memory replication to peers was chosen because it survives exactly the failure being
demonstrated - a coordinator that dies with jobs in flight - with no new failure modes.

**No replication; rely on the client resubmitting.** Rejected because it loses every job the
client has not yet been told about, which is precisely the set of jobs a coordinator failure
strands: the client believes it submitted them. Resubmission also needs an idempotency story
(the same job id must not run twice) which is the same state we would have replicated anyway,
only later and less reliably.

**Majority quorum instead of full replication.** Rejected for this cluster size: with three or
four workers a quorum of two already covers every job in practice, and the extra bookkeeping
(quorum bookkeeping, per-entry quorum) would be paid for a scenario the demo cannot produce. It
is the natural next step if the cluster grows; the entry format already carries what it would
need.

**A dedicated membership/gossip layer for failure detection.** Rejected: `RegistryService` plus
the heartbeat already provide discovery and liveness, and adding a gossip layer would duplicate
another member's work and add a consistency model the assignment does not need.

---

## 6. Events (B5)

New `WorkerEvent` kinds, recorded and visible in the client's existing feed:

`COORDINATOR_SUSPECTED`, `COORDINATOR_FAILED`, `FAILOVER_STARTED`, `FAILOVER_COMPLETED`,
`JOB_RECOVERED`, `CHECKPOINT_REPLICATED`, `LEADERSHIP_STEP_DOWN`.

`SystemLogPane` classifies lines by substring, not by kind, so no GUI change is required; the
detail strings are written so that classification lands sensibly ("failed" reads as an error,
"recovered" as recovery). If the team prefers explicit per-kind colouring, that is a small change
in the client owner's file.

---

## 7. Decisions

Every judgement call made along the way, and why.

1. **Transport seam instead of replacing RMI in place.** The election logic talks to an
   `ElectionTransport`; the gRPC transport is chosen once a worker publishes its endpoint, and
   the RMI transport remains the fallback. Less invasive than deleting the RMI path, and it
   keeps every existing election test meaningful while the cluster runs on gRPC.
2. **A failed hop returns `false` rather than throwing.** The caller's reaction to a dead peer
   and an unroutable peer is identical - drop the edge and rewire - so transport failures are
   data, not exceptions. This also removes a stack trace from the normal path.
3. **Cached channels invalidated on `UNAVAILABLE` but not on `DEADLINE_EXCEEDED`.** A peer that
   is unavailable may have restarted on a different port, so the cached channel is dropped and
   re-resolved; a peer that merely missed a deadline is slow, and its channel is still correct.
   Evicting on every slow hop would churn channels for no reason.
4. **Terms order announcements; they do not change the winner rule.** The assignment's rule is
   lowest JAC then highest id, and it stays exactly that. Terms only decide *which* verdict
   supersedes which, so a late wave cannot overwrite a newer election.
5. **A term-less (legacy RMI) announcement is applied unconditionally.** Under the old contract
   a rotation to a *lower* worker id is legal; ordering those would silently break the
   five-job term. Ordering therefore applies only to gRPC announcements.
6. **`is_coordinator=false` in a heartbeat reply means re-discover, not elect.** The peer
   answered, so the cluster is not silent; the cheapest correct action is to find out who leads.
7. **Acceptance waits for one peer, writes to all.** Waiting for every peer would let one slow
   replica delay a submission; writing only to the first that acknowledged (the first version)
   left other replicas unable to fold any later delta, because a delta carries no input. The
   fixed version pushes to every peer and waits for the first acknowledgement.
8. **A peer that does not answer the takeover fetch counts as gone.** A crashed worker remains
   in the directory until it exits, so the fetch doubles as the liveness signal the decisions
   need.
9. **Snapshots name the accepting coordinator, not the replica serving them.** The first version
   stamped the serving replica's id, which made every replicated job look abandoned and would
   have let a new leader steal a job its owner was still finishing. A test caught this.
10. **Recovered jobs bypass the term budget, and are not charged.** They were already accepted
    under the previous coordinator; see section 3.2.
11. **"Owner died later" is left to the existing slice probes.** Once a job is running, a slice
    whose worker dies is already detected and reassigned every 500 ms by Member 3's probe loop.
    A second detector for the same event would be duplicated machinery with its own failure
    modes, so the takeover covers only what the probes cannot see: a job whose *coordinator* died.
12. **The takeover runs on the worker's pool, not the gRPC handler thread**, and refuses new job
    submissions while it runs.
13. **Delivery is not deduplicated server-side.** Replaying a result the client may already have
    received is safe because `ClientApp.onResult` drops a second result for a job id it has
    already removed from its row map, and it costs no coordination. Deduplicating delivery
    exactly once across a coordinator failure needs a client-side acknowledgement loop that the
    client owner should own.
14. **`getProgressPort()` caches its value.** gRPC throws from `getPort()` once a server is
    down, and that value is read by every dispatch and probe - including ones racing a shutdown.
    An end-to-end test caught the resulting spurious "Dispatch failed" answer.
15. **`CheckpointSource` is an interface** so the takeover's decisions are unit-tested against
    known peer states instead of a live cluster.
16. **New `WorkerEvent` constants only.** No GUI file was edited; the client owner may want to
    add colours.
17. **Two rules were extracted rather than left inline in the shared worker file**, so they could
    be tested without a cluster: `FailureGate` (one election per failure) and `LeaderView`
    (whether a peer's reported leadership replaces ours). Both were written first as private
    logic in `WorkerRemoteImpl`, and reviewing them found two real defects - see section 9.

---

## 8. Known gaps

Stated plainly, because a demo will find them.

1. **Replication is not quorum-checked.** A peer that acknowledges an acceptance and then loses
   it (crash before its own flush of later deltas) still counts as a replica. With three or
   four workers this does not lose jobs, but it is not a majority guarantee.
2. **A job whose acceptance reached no peer is lost with the coordinator.** If every peer is
   unreachable at acceptance, the coordinator logs a warning and proceeds; that job cannot be
   recovered afterwards. The alternative - refusing the job - was rejected because an
   unavailable replica should not stop a cluster that has live workers, but the trade-off is
   real and should be a conscious team decision.
3. **The failover path is not exercised by a scripted chaos run.** The automated test kills one
   coordinator mid-job; repeated kills, kills during a takeover, and kills during an election
   are covered by hand only.
4. **Progress streams are not reconnected** after a failover. The client sees the recovered job
   on the new coordinator, but a stream opened against the dead coordinator simply ends. The
   client's reconnect is the client owner's work.
5. **A zombie coordinator steps down when it hears a higher term.** It does not proactively
   verify its own leadership before serving: a job submitted to it in the window between the
   election and the heartbeat reply may be refused with a retryable error rather than run. That
   is the designed contract for a leadership change, not a data-loss path.
6. **Retention is a count, not a time.** 256 finished jobs per replica regardless of age.
7. **`getRecentEvents` overflow is silent**, as it already was before this work: a client that
   falls more than 64 events behind on one worker misses the prefix.
8. **No disk persistence**, by choice (section 5). A whole-machine restart loses running jobs.
9. **A graceful shutdown during a running job can deliver an error to the client.** The worker
   closes its job service while slices are still in flight, a pending slice recovery is rejected
   by the closing thread pool, and the job fails with an internal message such as "Recovery could
   not start". A *crashed* process cannot do this, so the demo's crash path is unaffected, but a
   `Ctrl+C` during a job can show it. The clean message would be a small change in the job
   service's shutdown handling and belongs to that file's owner; it is listed here rather than
   fixed, to keep the shared-file diff small.
9. **The legacy RMI election path is still reachable** for a worker that never publishes a gRPC
    endpoint. It is a fallback for a pre-migration cluster, not a second supported mode.
10. **One best-effort RMI read remains in the wave path**: the first wave from a new neighbour
    seeds the 2-hop repair cache by asking that peer for its neighbour list. It is topology, not
    protocol - it is seeded at connect time anyway, and `GrpcElectionTest` converges with the RMI
    registry stopped entirely, which is what shows the protocol itself is gRPC-only. It will go
    when peer links migrate.

---

## 9. Defects found by reviewing this work

Recorded because the first version of this work passed its own tests and was still wrong in
these ways. Each now has a test that fails without the fix.

1. **Storm suppression never expired.** The suppression key was `coordinatorId + "@" +
   FAILOVER_SUPPRESSION_MS` - a constant, not a timestamp - so the *same* worker failing again
   was suppressed permanently. Killing one worker, restarting it, and killing it again would have
   left the cluster with a dead coordinator and no election. `FailureGate` keys on
   (coordinator, when) and is unit-tested with an injected clock.
2. **The `is_coordinator=false` case was documented but not implemented.** Only a newer term
   triggered re-discovery, so the "the coordinator I am probing says it is not the coordinator"
   signal - which the report explicitly claims - was being read and thrown away.
   `LeaderView.decide` now implements it and is unit-tested.
3. **`getProgressPort()` threw after shutdown**, failing any dispatch that raced a teardown.
   Caught by the end-to-end test.
4. **Replication stopped at the first peer that acknowledged**, so peers that missed the
   acceptance could never fold a later delta and were permanently unable to take that job over.
5. **A checkpoint snapshot named the serving replica as the job's coordinator**, making every
   replicated job look abandoned and letting a new leader steal a job its live owner was still
   finishing.
6. **The takeover trusted the directory as proof of life.** A crashed worker stays registered
   until it exits, so recovery had to learn liveness from the takeover fetch instead.