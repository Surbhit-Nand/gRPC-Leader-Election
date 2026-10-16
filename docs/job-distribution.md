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
not yet published a ready job listener. New workers participate in subsequent jobs.

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

Election, heartbeat probes, peer links and cluster-monitor queries still use the
existing RMI implementation. The registry also uses that contact for peer selection.
The legacy RMI job API remains for compatibility tests, but the updated GUI's job
submission, task dispatch, result reporting and callback path use gRPC exclusively.
This is the computation integration, not a claim that the entire A2 migration is done.

Worker reassignment/recovery belongs to `worker-failure-recovery`. This branch
reports assignment failures and times out unfinished jobs after ten minutes; it
does not retry or reassign slices. A timeout is a bounded error path, not a failure
detector. Running slices may finish after a job fails. Callback failure is logged;
there is no durable result replay or coordinator crash recovery yet. Deduplication
state is in memory and retained until the worker process exits.

## Verification

```powershell
mvn test
```

GrpcJobDistributionTest combines the real shared registry, job services, client
callback and progress stream over TCP. It covers all operations, long sums,
concurrency, duplicate IDs, endpoint publication, joins, invalid inputs and
unreachable workers. Existing bootstrap and frozen-contract tests remain applicable.
