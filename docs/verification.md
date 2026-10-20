# Verification

Last full run: 20 October 2026.

| Item | Value |
| --- | --- |
| Command | `mvn -B verify` |
| JDK | Eclipse Temurin 25.0.4.1 |
| Maven | Apache Maven 3.9.6 |
| Reactor | shared, bootstrap, worker, client, integration-tests |
| Result | **BUILD SUCCESS** |
| Tests | **179 run, 0 failures, 0 errors, 0 skipped** |
| Coverage | JaCoCo 0.8.15; aggregate report at `integration-tests/target/site/jacoco-aggregate/index.html` |

The suite includes real gRPC TCP calls, process-level worker JVMs, a forcibly
terminated coordinator, stream reconnection, worker reassignment, bootstrap
lease expiry, persistent ID counters, bounded retention, the multi-worker
splitter scale check, and the no-RMI guard. The GitHub Actions workflow
`.github/workflows/verify.yml` runs the same command on JDK 25 and uploads
Surefire and JaCoCo artifacts.

## Focused commands

```text
mvn "-Dtest=GrpcOnlyClusterTest" test
mvn "-Dtest=GrpcControlRegressionTest,ReplicationAdmissionTest" test
mvn "-Dtest=BootstrapStateStoreTest,BootstrapRegistryLeaseTest,BoundedRetentionTest" test
```

Use the full reactor (`-am` or the root command) when running a focused
integration-test command: the integration module resolves the sibling modules
from the current reactor.
