# gRPC-only module migration

Updated 20 October 2026 (Pacific/Fiji), with OpenAI Codex assistance.
This records the changes made in response to the requirement review; the earlier
[AI-assisted audit](ai-assisted-audit.md) describes the historical mixed codebase.

## Changes

- Removed Java RMI imports, object exports, registry lookups, the fallback election
  transport and the alternate Java-object job-distribution path from production.
- Kept the existing gRPC services as the network implementation. Renamed the
  production local APIs to `WorkerControl`, `ClusterDirectory` and `WorkerRuntime`.
  Deprecated `WorkerRemoteImpl` and `BootstrapRemote` aliases remain for source
  compatibility; they do not enable another transport.
- Created separate `bootstrap`, `worker` and `client` Maven projects, supported by
  `shared` and `integration-tests`. Sources physically belong to their modules.
- Required a confirmed peer checkpoint before acknowledging new work. Rejected
  attempts start no computation and release their term reservation. The GUI retries
  retryable failures using the same job ID.
- Replaced obsolete RMI harness tests with gRPC control, topology, term-budget and
  admission regression tests. Added a guard test that rejects RMI imports and
  RMI supertypes, plus minority-partition and quorum-lease election tests. Existing
  arithmetic, distribution, progress, recovery, election and failover coverage
  remains in the integration-test module.
- Updated the launcher, IDE launch configurations, CLI defaults and current
  architecture/setup documentation.

## Review notes

The three application modules depend only on `cluster-shared`, not on each other.
Generated protobuf sources exist only in the shared module. The deprecated aliases
are local Java types and do not extend `java.rmi.Remote`; the guard test verifies
that no production source imports RMI.

Acceptance is replicated to at least one peer, not durably committed to a quorum.
See [failover limits](failover.md#guarantees-and-limits) for the remaining failure
model and retry ambiguity.

Run `mvn verify` for validation and
`mvn package dependency:copy-dependencies -DincludeScope=runtime` to prepare the
module-specific launch classpaths. No applications need to remain running after
the tests finish.
