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
