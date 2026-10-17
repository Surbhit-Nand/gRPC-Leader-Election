# Job distribution integration

The `job-distribution` branch now uses the shared `job.proto`, `registry.proto`
and `progress.proto` contracts from main. The temporary `computation.proto`
contract and standalone manual-address demo have been retired.

## What is connected

- Workers publish their job/progress gRPC listener through RegistryService.
- RegistryService allocates cluster-wide integer job IDs for clients.
- The GUI submits jobs using JobService.SubmitJob.
- The coordinator takes a fresh registry snapshot, splits input into balanced
  non-empty slices, and includes itself in computation.
- Remote slices use AssignTask; answers return through ReportTaskResult to the
  original coordinator, even if leadership changes during the job.
- ClientService.DeliverResult supplies the aggregated answer to the GUI.
- WatchJob streams actual completed units with the existing worker breakdown.
- MAX and PRIMECOUNT retain int32 results; PRIMESUM retains int64 results.
- The existing leadership and five-job term budget gate submissions; each remote
  assignment increments JAC. Concurrent clients use separate callback ports.
- Identical duplicate submissions are acknowledged without redispatch; conflicting
  reuse of a job ID is rejected. Duplicate results do not increase progress twice.

## Endpoint publication

All original service/method names and field numbers remain unchanged. Additive
registry fields distinguish the two ports during the migration:

- WorkerEndpoint.grpc_port (field 4): job/progress gRPC listener; zero until ready.
- RegisterRequest.grpc_port (field 5): optional gRPC listener metadata.
- RegisterRequest.update_endpoint (field 6): publish after the auto-assigned worker
  ID is known. The ID, host and RMI contact port must match the registered entry.

WorkerEndpoint.port still identifies the RMI contact used by the existing peer
network. Job RPCs use grpc_port exclusively. This avoids guessing a port or sending
HTTP/2 traffic to an RMI registry. A directory snapshot excludes workers that have
not yet published a ready job listener. New workers participate in subsequent jobs and can receive unfinished slices during recovery.

## Run

Rebuild and restart bootstrap, workers and clients together; old running JVMs do
not gain new services when sources are compiled. Use one terminal per process:

```powershell
.\run.ps1 build
.\run.ps1 bootstrap
.\run.ps1 worker localhost 1099 grpc
.\run.ps1 worker localhost 1099 grpc
.\run.ps1 worker localhost 1099 grpc
.\run.ps1 client localhost 1099 1199
```

Wait for each worker's `gRPC jobs and progress on port ...` startup line before
starting the next worker. The existing bootstrap peer-selection path can prune a
worker that registered but has not bound its RMI contact during simultaneous startup.

The client arguments are `host bootstrapRmiPort callbackGrpcPort bootstrapGrpcPort`;
the last defaults to 50051. A second client can use callback port 1200. For custom
bootstrap ports, pass the gRPC port to both the worker and client:

```powershell
.\run.ps1 bootstrap 21099 51051
.\run.ps1 worker localhost 21099 51051
.\run.ps1 client localhost 21099 21199 51051
```

Example jobs: MAX `12,4,99,-3,8,17` returns 99; PRIMESUM `1,1000000` returns
37550402023; PRIMECOUNT `2,3,4,5,8,11` returns 4. View opens each job's worker
progress breakdown. The GUI retains its CSV controls and input validation.

## Remaining work

Election, legacy network heartbeat probes, peer links and cluster-monitor queries still use the
existing RMI implementation. The registry also uses that contact for peer selection.
The legacy RMI job API remains for compatibility tests, but the updated GUI's job
submission, task dispatch, result reporting and callback path use gRPC exclusively.
This is the computation integration, not a claim that the entire A2 migration is done.

## Worker failure recovery

Each slice has a stable task ID and an attempt number. Results must match the
current slice, attempt and worker; duplicate and delayed results are acknowledged
without changing aggregation. A worker can execute multiple slices of one job.
The additive protobuf fields preserve existing field numbers and RPC names;
rebuild all workers together because recovery requires echoed task/attempt IDs.

While a remote slice is unfinished, its coordinator repeats the same AssignTask
RPC every 500 ms, with a one-second deadline and at most one probe in flight per
slice. Workers acknowledge repeated assignments without repeating computation;
a completed attempt resends its cached result. This detects a stopped or
unreachable worker even after the original assignment was acknowledged, and also
recovers a lost result callback. A slow calculation with a responsive gRPC server
continues on its original worker. Network delays can trigger reassignment, but
attempt fencing prevents either copy from being counted twice.

On RPC failure, the coordinator fences the attempt, refreshes discovery through
gRPC RegistryService, and assigns the slice to a surviving remote worker,
preferring one with fewer outstanding slices. Newly joined workers are eligible.
Failed workers are excluded for that job. If discovery is unavailable or no remote
candidate remains, the coordinator computes the slice itself. Subsequent jobs
refresh membership independently. Probes, discovery, reassignment, partial
results and delivery to the original client use gRPC, not RMI.

Progress transfers unfinished units to the replacement worker atomically and
preserves completed units. The original coordinator aggregates each slice once
and sends MAX, PRIMESUM or PRIMECOUNT to the original submitting client, including
after normal leader rotation. The existing ten-minute overall deadline still
bounds a stalled job; recovery does not extend it.

Recovery state and cached worker results are in memory until process exit.
Coordinator crash recovery and durable final-client callback replay are outside
this feature. Final callback failure is logged. Repeated assignments currently
resend the slice payload; a lightweight dedicated heartbeat would reduce traffic
for large inputs.

## Verification

```powershell
mvn test
```

GrpcJobDistributionTest combines the real shared registry, job services, client
callback and progress stream over TCP. It covers all operations, long sums,
concurrency, duplicate IDs, endpoint publication, joins, invalid inputs and
unreachable workers. Existing bootstrap and frozen-contract tests remain applicable.

GrpcWorkerRecoveryTest uses real TCP gRPC servers without RMI. It stops workers
after they accept active tasks, adds a worker mid-job, injects stale/duplicate
results, verifies all three operations and progress, checks multiple failed
slices with local fallback, and confirms that slow responsive workers keep their
work without recalculation.

## Recovery rationale and demonstration

The failure detector treats an RPC error or missing acknowledgement as suspicion,
not proof of a crash. A crashed process and a network partition cannot be
distinguished from a single timeout. Reassignment therefore permits duplicate
execution, while task/attempt/owner checks allow each slice to affect aggregation
only once. This matters especially for PRIMESUM and PRIMECOUNT: adding both copies
would silently produce the wrong answer. MAX also rejects stale ownership rather
than relying on its aggregation being insensitive to equal duplicates.

The 500 ms probe interval and one-second deadline favour quick recovery for a
local demonstration. A silent endpoint is normally suspected within roughly
1.5 seconds, excluding scheduling and recovery discovery delays. On a slower
network these constants should be increased to avoid unnecessary recomputation.
Acknowledgements are independent of calculation completion, so a long-running
prime range does not itself trigger reassignment. Probe messages reuse cached
protobuf assignments, and workers decode only newly accepted attempts.

To demonstrate deterministically, run:

```powershell
mvn '-Dtest=GrpcWorkerRecoveryTest' test
```

The tests include a server that remains connected but stops answering assignment
RPCs, recovery of three concurrent jobs, a PRIMESUM range whose answer exceeds
32 bits, and a lost result callback that must be replayed without recalculation.
These complement the stopped-server, new-worker and stale-result scenarios.

For a manual demonstration, start bootstrap, at least three workers and a client
using the commands above. Submit a sufficiently long PRIMESUM range, then close
a non-coordinator worker while its progress row is still Processing. Observe the
coordinator log identifying the job, task, failed worker, replacement worker and
new attempt number. The client progress breakdown should transfer unfinished
units to the replacement and eventually deliver the correct answer. Repeat the
same input without failure to compare results. Starting another worker before a
failure lets it receive reassigned work when discovery includes its published
endpoint. Do not close the original job coordinator for this demonstration:
recovering its job state is the separate coordinator-failure feature.
