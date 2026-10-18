# AI-assisted audit and regression fixes

This audit and the associated fixes/tests were produced with OpenAI Codex assistance
at the repository owner's request on 18–19 October 2026 (Pacific/Fiji). The owner
should review the diffs and use this record in the assignment's AI-use disclosure;
it is not a claim that these changes were independently written by a student.
Existing authorship and history are preserved. No commits are pushed automatically.

Baseline: clean `test-branch`, `c441442`; Oracle JDK 25.0.4.1, Maven 3.9.11;
227 tests passed. Maven was found in the user's `.m2/wrapper/dists` cache.
OneDrive read-only generated directories blocked the ordinary build. Validation
uses a temporary copy of `pom.xml` outside OneDrive with only build-output,
source, test-source and proto-source paths changed. All dependencies, compiler
settings and test configuration remain those in the repository. Test sources
are read directly from this repository, not a stale source copy.

## Completed-slice recovery

**High severity:** `GrpcJobService.Run.dispatch` skipped every restored slice
with a result, but only a fresh completion called aggregation. A crash between
the final slice checkpoint and the finished checkpoint left a fully computed job
waiting until its deadline. The regression
`GrpcJobDistributionTest.recoveredJobWithEverySliceCompletedDeliversWithoutRecomputing`
failed before the fix: expected `17`, received `Job timed out waiting for worker results`.
Dispatch now checks for aggregate completion, sharing the normal completion path.
The test also checks no remote assignments, finished progress and active-job cleanup.

## Finished-result replay

**High severity:** `WorkerRemoteImpl.tombstoneResult` returned the first slice
result, discarding the final aggregate (and potentially a saved error). A real
gRPC regression seeded a finished checkpoint and elected the survivor: expected
`17`, received `10`. `RestoredJob` now carries the saved final `TaskResult` through
the takeover to the client callback without deriving it from partial results.

## Replay acknowledgement

**High severity:** takeover marked a job delivered even when its deliverer returned
false; the gRPC deliverer itself returned true before the callback completed.
The regression `failedReplayRemainsUndeliveredAndCanBeRetried` failed on the original
code. Replay now waits for a bounded gRPC acknowledgement and retains failed
deliveries for a later takeover pass. `replayReportsFailureUntilClientAcknowledges`
also exercises a real rejecting callback and a successful callback. Takeover passes
are serialized so their idempotence set is safe under concurrent triggers.

## Progress after partial failure

**Medium severity:** a worker may own several slices after reassignment. Marking
an unfinished slice failed reset that worker's previously completed units to zero.
The real streaming regression in `JobProgressStreamingTest` reproduced `3 -> 0`.
Failed work now preserves completed units while changing the worker status to Failed.

## All-gRPC executable paths

**High severity / assignment compliance:** WorkerNode looked up BootstrapRegistry
over RMI before its `--grpc` branch; bootstrap peer selection, joining, repair and
ClientApp monitoring also made RMI calls. The actual worker process exited with
`Connection refused` against a gRPC-only bootstrap in `GrpcOnlyClusterTest`.

`control.proto`, `WorkerControlService` and `GrpcWorkerClient` carry topology and
status/events as typed protobuf messages. RegistryClient also adapts the historical
local directory interface to gRPC. Bootstrap gRPC registration makes no peer
network calls under its registration lock; joiners probe published endpoints with
deadlines. WorkerNode publishes its listener, refreshes membership, joins via gRPC
and adopts the peer's leadership term. The executable bootstrap and workers no
longer export RMI objects. Old constructors and RmiElectionTransport remain for
legacy regression fixtures, and are not selected by the application entry points.
Legacy positional contact ports remain accepted but unused; `--grpc-port` and the
documented bootstrap/client gRPC port arguments select the actual endpoints.

Verification includes actual worker JVMs, a bootstrap with no exported RMI object,
dynamic joins, gRPC monitoring, a forcibly terminated coordinator, correct recovered
PRIMESUM and completed progress. The historical regression suite is retained.

## Snapshot sequence consistency

**High severity:** finished snapshots invented sequence `N + 1` for their terminal
entry. Fetching a snapshot could therefore suppress the next genuine delivery
acknowledgement at that same sequence. `finishedSnapshotDoesNotConsumeTheNextDeliverySequence`
reproduced source sequence `2` becoming replica sequence `3`. Snapshot construction
now uses `N - 1` for the input envelope and `N` for the terminal marker, preserving
the actual high-water mark and accepting the subsequent delivery update.

## Missing terminal markers during takeover

**High severity:** `WorkerRemoteImpl.snapshots` filtered out finished, delivered jobs.
A survivor holding an older unfinished checkpoint could therefore never learn that
another peer knew the job was already finished. The original regression returned an
empty fetch for a retained delivered job. Fetch now includes all retained jobs,
including terminal markers; the test confirms an older replica becomes finished
and delivered. Existing bounded checkpoint retention still applies.

## Out-of-order replication

**High severity:** asynchronous per-job delta RPCs could arrive before acceptance
or before an earlier slice update. The log's highest-sequence filter then permanently
discarded required state. `CheckpointReplicationTest` captures actual gRPC pushes
from a running coordinator and folds them in reverse arrival order; the original
code lost the finished job. Each push now carries the cumulative job snapshot.
Sequence allocation, local folding and snapshot capture are atomic, with network
I/O outside the lock. Log reads return detached job views so concurrent incoming
replication cannot mutate a recovery reader's slice collection.

Tradeoff: full snapshots send more bytes than deltas. This favors correctness and
explainability for the small assignment cluster; replication is still best effort,
in memory, and cannot guarantee recovery when every replica misses the state.
