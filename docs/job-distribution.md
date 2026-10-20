# Job distribution and streamed progress

Contracts are in `shared/src/main/proto/`. `GrpcJobService` and `ProgressService`
are in the worker module. The client module owns the Swing progress view and
subscription/reconnection logic.

## Submission

The client asks bootstrap for a unique job ID, discovers the coordinator, and
sends a typed `SubmitJobRequest` containing its input and callback endpoint.
The coordinator validates the request, claims the job ID and reserves one
leadership slot. It splits the input into balanced nonempty slices and replicates
the acceptance snapshot before acknowledging or dispatching work.

At least one peer must confirm the snapshot sequence. With no confirmed replica,
the RPC returns `UNAVAILABLE`, starts no task and releases the reserved term slot.
Retries use the same job ID and payload. An acknowledgement lost on the network
can leave an uncertain outcome, so inventing a new job ID is not a safe retry.

## Work and results

- MAX accepts an integer list and combines partial maxima.
- PRIMESUM accepts a list or inclusive range and uses 64-bit sums.
- PRIMECOUNT counts primes, including repeated values in a list.
- Small inputs use fewer participants to avoid empty MAX slices.

Each slice has a stable task ID, owner and attempt. Task execution is deduplicated
by `(job ID, task ID, attempt)`. Results with obsolete ownership or attempts cannot
complete a replacement task. JAC increments when remote assignments are issued.

The coordinator probes outstanding assignments. Unreachable workers have their
unfinished slices reassigned using fresh discovery; a late-joining worker can
receive this work. When no suitable peer remains, a slice can run locally.
Completed slice results are retained and included once in the aggregate.

The aggregate is delivered to the client's gRPC callback service. Client processes
must use distinct callback ports. Callback failures are retained for replay during
takeover rather than falsely recorded as acknowledged deliveries.

## Progress

`JobProgressService.WatchJob` is a server stream of overall and per-worker
completed/total units and status. The GUI updates on Swing's event thread.
Clicking a progress bar opens the worker breakdown. Completion reflects finished
slices, so a long slice may show no change until its result arrives.

Interrupted subscriptions retry with bounded delays. They rediscover published
workers and locate the job's new owner after coordinator failure without submitting
the job again. Reassignment preserves units already completed by an affected worker.

Submission and task-deduplication maps are bounded (1024 jobs and 4096 task
attempts). This keeps a long-lived coordinator's memory predictable; a duplicate
submission older than that retention window may be treated as new work.

## Verification

```text
mvn "-Dtest=GrpcJobDistributionTest,GrpcWorkerRecoveryTest,JobProgressStreamingTest,JobProgressReconnectTest,GrpcControlRegressionTest" test
```

These tests cover real TCP jobs, duplicates, stale results, late joins, dropped
callbacks, slow healthy workers, failed workers, progress preservation and
reconnection. Arithmetic and splitting have separate unit tests.
