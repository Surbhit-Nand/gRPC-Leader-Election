# Member 3: job distribution

Branch: `job-distribution`, based on `progress-bar` at `8d1a510`.

This feature implements the computation services for Assignment 2. It is not a
complete migration of the application: the existing GUI, bootstrap discovery,
election and legacy submission path still use RMI. Those paths remain available
while the group integrates its features. The new computation services use only
Protocol Buffer messages and gRPC for remote task dispatch and results.

## Implemented

- `computation.proto` defines `JobService.SubmitJob` (server streaming) and
  `TaskService.ExecuteTask` (unary).
- `GrpcJobService` snapshots membership for each new job, splits work evenly,
  includes the coordinator in computation, dispatches peers concurrently through
  asynchronous gRPC stubs, and combines numeric results.
- `GrpcTaskService` computes slices on a supplied Java executor. The standalone
  `DistributionNode` hosts it without changing the existing RMI `WorkerNode`.
- MAX and PRIMECOUNT accept integer lists; PRIMESUM accepts an inclusive integer
  range. Empty MAX and reversed ranges are rejected. Empty PRIMECOUNT is zero.
- UUID job IDs separate clients. Task IDs are stable within a job (`job-id/index`).
  Responses must match the requested job, task and worker IDs.
- Each streamed snapshot includes worker IDs, task IDs and completed/total units.
  Units advance only when a slice finishes; there is no artificial timer.
  A successful final snapshot contains `finished=true` and a present `result`.
- A fresh discovery snapshot lets workers joining during one job participate in
  subsequent jobs; an already partitioned job retains its original participants.

## Run independently of Member 1

This branch owns computation only. No bootstrap implementation, discovery RPC,
election algorithm or GUI changes are needed to run the following demo. The
computation-specific protobuf file is a local contract for your feature; adapt
or consolidate it with Member 1's shared contract during integration.

Build from the project root:

```powershell
mvn compile dependency:copy-dependencies "-DincludeScope=runtime"
```

Start each node in a separate terminal, workers first:

```powershell
java -cp "target/classes;target/dependency/*" com.cs324.computation.demo.DistributionNode worker 2 51002
java -cp "target/classes;target/dependency/*" com.cs324.computation.demo.DistributionNode worker 3 51003
java -cp "target/classes;target/dependency/*" com.cs324.computation.demo.DistributionNode coordinator 1 51001 2@localhost:51002 3@localhost:51003
```

In another terminal, submit jobs:

```powershell
java -cp "target/classes;target/dependency/*" com.cs324.computation.demo.DistributionClient localhost 51001 MAX 12 4 99 -3 8 17
java -cp "target/classes;target/dependency/*" com.cs324.computation.demo.DistributionClient localhost 51001 PRIMESUM 1 100
java -cp "target/classes;target/dependency/*" com.cs324.computation.demo.DistributionClient localhost 51001 PRIMECOUNT 2 3 4 5 8 11
```

Expected results: `99`, `1060`, `4`. Each client prints overall completion and
worker-level completed units. Run clients in different terminals to submit
concurrently. Stop each node with Ctrl+C when finished.

The demo uses a fixed coordinator and fixed addresses to test your component.
It does not implement discovery, automatic joins, elections or five-job term
rotation. The service's membership supplier is the integration point for the
live directory later. The existing GUI remains separate from this console demo.

## Integration contracts

**Member 1:** review `src/main/proto/computation.proto` alongside your shared
protobuf foundation. The Maven protobuf generator already compiles it. Publish
each worker's computation gRPC host and port through the new bootstrap service.
Do not send its RMI registry port to `GrpcJobService.Endpoint`.

**Member 2:** attach `GrpcJobService` to the worker's gRPC server alongside
`GrpcTaskService`. Its constructor accepts:

```java
new GrpcJobService(
    workerId,
    () -> discoverySnapshot,       // List<Endpoint>, refreshed on each call
    () -> coordinatorAcceptsJobs,  // leadership/term admission hook
    targetId -> incrementJac(),    // one increment per remote assignment
    computationExecutor,
    Duration.ofMinutes(10));       // configurable per-task deadline
```

The coordinator always includes itself locally. The supplied peer list may include
it; that entry is ignored. Other worker IDs must be unique. The executor should
have multiple threads. The host owns and shuts down the executor and gRPC server.
The service owns and closes its outbound channels.

The leadership admission hook must implement the existing term-budget policy
(atomic reservation and handover gating). This component does not elect leaders,
count five-job terms or replicate coordinator state. Existing accepted jobs remain
attached to their original coordinator service when leadership changes normally.
Coordinator crash recovery belongs to Member 2.

**Member 4:** call `JobServiceGrpc.newStub(channel).submitJob(request, observer)`
with a fresh UUID per job. Use `JobUpdate.tasks` for the worker breakdown. Overall
progress is `100 * sum(completed_units) / sum(total_units)`; for an empty job show
0 until `finished`, then 100. Snapshots advance at slice completion, not per number.
Use the final optional result rather than a separate Java callback. Surface gRPC
errors through `onError`, and update Swing controls on the EDT.

The legacy GUI is not yet connected to this service. The existing
`JobProgressService.WatchJob` stream and its integer job IDs are separate from
this UUID-based submission stream; do not assume those APIs share state.

## Recovery branch boundary

Create `worker-failure-recovery` from the completed job-distribution changes (or
from main after they merge), so it inherits this contract. That branch will add
task attempt tracking, retry/reassignment, stale-result rejection and recovery
tests. This branch currently terminates a job stream on task RPC failure or
deadline expiry; it does not claim worker failure tolerance.

Only concurrent duplicate job IDs are rejected. Completed jobs are not retained
for replay, and task execution is not yet deduplicated. Do not automatically retry
submissions with this version. Cancellation closes outbound channels, but an
already running calculation may finish on the worker. Deadline choice must allow
the largest supported range; it is not a heartbeat-based failure detector.

## Validation

```powershell
mvn "-Dtest=GrpcJobDistributionTest,WorkerTaskDistributorTest,WorkerTaskCalculatorTest" test
```

The 22 tests cover real TCP gRPC serialization and streaming, all three operations,
long prime sums, negative maxima, empty inputs, membership changes, concurrent
clients, admission rejection, incorrect task identities, local-only operation and
transport failure, plus existing split/calculation tests. Automated tests use
multiple network servers in one JVM; the console demo supports separate processes.
Bootstrap/election/GUI acceptance testing remains necessary after group integration.
