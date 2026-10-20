# CS324 Assignment 2 (2026): Distributed Worker Cluster

A Java cluster for MAX, PRIMESUM and PRIMECOUNT. Discovery, peer connections,
elections, assignments, results, monitoring, progress streaming and checkpoint
replication all use Protocol Buffers and gRPC. There is no RMI implementation,
registry, object export or fallback in the applications.

## Projects

The root Maven POM aggregates five separate Maven projects:

| Directory | Artifact | Responsibility |
| --- | --- | --- |
| `shared` | `cluster-shared` | Protobuf contracts, generated clients, local data interfaces, arithmetic and splitting helpers |
| `bootstrap` | `cluster-bootstrap` | Worker discovery and cluster-wide ID allocation |
| `worker` | `cluster-worker` | Election, computation, streamed progress and failover |
| `client` | `cluster-client` | Swing GUI, job submission, callbacks and monitoring |
| `integration-tests` | `cluster-integration-tests` | Unit and cross-application regression tests |

Each application has its own POM, sources, compiled classes, JAR and runtime
classpath. Bootstrap, worker and client depend on `cluster-shared`; they do not
depend on each other's implementation. Tests depend on all three applications.
Production code uses `WorkerControl`, `ClusterDirectory` and `WorkerRuntime`.
Deprecated `WorkerRemoteImpl` and `BootstrapRemote` aliases remain for source
compatibility; both are plain local Java types and do not use `java.rmi`.

## Prerequisites

- JDK 25 or newer. Check both `java -version` and the Java version in `mvn -version`.
- Maven 3.9 or newer.
- A graphical desktop for the Swing client. Tests run without opening the GUI.

Use the same JDK in `JAVA_HOME`, Maven and your IDE.

## First-time setup

```text
git clone https://github.com/Surbhit-Nand/gRPC-Leader-Election.git
cd gRPC-Leader-Election
mvn package dependency:copy-dependencies -DincludeScope=runtime
```

This runs the tests, generates protobuf/gRPC sources in `shared/target`, and
packages the projects. Each application's dependencies go into its own
`target/dependency` directory. The first build needs access to Maven Central.

To compile and package without running tests:

```text
mvn package -DskipTests dependency:copy-dependencies -DincludeScope=runtime
```

The equivalent Windows shortcut is `.\run.ps1 build`. It skips execution of tests;
use `mvn verify` to run the complete suite.

## Run a cluster

Open a separate terminal for each process, starting in the repository root.
Use **at least two workers**: new jobs require a checkpoint acknowledgement from
another worker. Use three or four workers for failure demonstrations.

1. Start bootstrap:

   ```powershell
   .\run.ps1 bootstrap
   ```

2. Start a worker in each of three additional terminals:

   ```powershell
   .\run.ps1 worker
   ```

3. Wait for the workers to report a common coordinator. An election starts
   automatically after the startup delay, normally about three seconds.

4. Start the client:

   ```powershell
   .\run.ps1 client
   ```

5. Choose MAX, PRIMESUM or PRIMECOUNT, enter the input, and submit. Click the
   progress bar to inspect each worker's completed and total units.

The worker menu supports `[1] Start Election`, `[2] Show Leader` and `[3] Exit`.
Use `[3]` for a graceful departure with neighbour repair. Terminating a worker
process simulates a crash.

### Direct Java commands

These Windows commands are equivalent to the helper script:

```powershell
java -cp "bootstrap/target/classes;bootstrap/target/dependency/*" com.cs324.bootstrap.BootstrapNode
java -cp "worker/target/classes;worker/target/dependency/*" com.cs324.worker.WorkerNode
java -cp "client/target/classes;client/target/dependency/*" com.cs324.client.ClientApp
```

On Linux/macOS, use `:` instead of `;` in each classpath. Each command is a
separate long-running process. The old root `target/classes` is no longer a
runtime classpath; use the appropriate application directory.

### Ports and optional arguments

| Application | Arguments | Defaults |
| --- | --- | --- |
| Bootstrap | `[grpcPort]` | `50051` |
| Worker | `[bootstrapHost] [bootstrapGrpcPort] [--grpc-port N]` | `localhost 50051` |
| Client | `[bootstrapHost] [callbackPort] [bootstrapGrpcPort]` | `localhost 1199 50051` |

Workers receive monotonic IDs from bootstrap and bind available gRPC ports
automatically. `--grpc-port` overrides the worker's positional bootstrap port.

For a second client, use a different callback port:

```powershell
.\run.ps1 client localhost 1200
```

For a cluster on bootstrap gRPC port 50060:

```powershell
.\run.ps1 bootstrap 50060
.\run.ps1 worker localhost 50060
.\run.ps1 client localhost 1199 50060
```

The supplied launcher and callback advertisement assume all processes run on
the same machine. This is a local demonstration setup.

### IDEs and individual Maven projects

Import the root `pom.xml` as a Maven project and allow the IDE to import all
modules. Set the project JDK to 25 or newer, then build once to generate the
protobuf classes. Launch the main class from its application module:

- Bootstrap: `com.cs324.bootstrap.BootstrapNode`
- Worker: `com.cs324.worker.WorkerNode`
- Client: `com.cs324.client.ClientApp`

Workers need an interactive console; use the terminal commands if an IDE's
debug console closes standard input. For NetBeans, select the intended
application project before using Run Project.

To make shared artifacts available for separate project builds:

```text
mvn install -DskipTests
mvn -f bootstrap/pom.xml compile exec:java
mvn -f worker/pom.xml compile exec:java
mvn -f client/pom.xml compile exec:java
```

Run those last three commands in separate terminals. Use the direct Java
commands for explicit process ownership during crash demonstrations.

## Behaviour and recovery

- **Election:** the lowest job-allocation counter (JAC) wins; ties favour the
  highest worker ID. Duplicate waves receive an echo, while duplicate
  announcements stop propagating. Terms prevent older leaders reclaiming control.
  A winner is announced only when the wave reaches a strict majority of ready
  workers.
- **Leadership budget:** a term accepts at most five distinct client jobs.
  The fifth accepted job triggers an election. JAC increments per remote
  assignment, separately from the per-job term budget.
- **Worker failure:** stable task IDs and increasing attempts reject stale results.
  Discovery is refreshed before reassignment, so newly joined workers can receive
  unfinished slices. Previously completed slices remain counted once.
- **Progress:** the server streams completed units; the GUI can show a worker
  breakdown. Interrupted streams reconnect and rediscover a recovered job's owner.
- **Coordinator failure:** survivors detect missed heartbeats, elect a leader,
  merge checkpoint snapshots and resume unfinished work or replay a saved result.
- **Split-brain guard:** a leader renews a majority lease with bounded status
  probes. A leader that cannot reach a majority stops accepting jobs and steps
  down; two isolated minority groups cannot both elect or keep serving.
- **Acceptance:** the coordinator acknowledges a submission only after a peer
  confirms the acceptance snapshot. Missing peers, failed replication and stale
  acknowledgements cause `UNAVAILABLE` before computation starts. The client
  retries the same job ID; rejected attempts release their leadership slot.

Replication remains in memory and requires at least one surviving replica.
This is not a durable consensus log: simultaneous loss of the coordinator and
all replicas can lose work. Leadership is lease-based rather than Raft/Paxos,
so a majority is required for availability, and losing it reduces availability
until enough workers return. A lost RPC acknowledgement leaves
the submission outcome uncertain; retry the **same job ID and payload**, because
a peer may already have stored it. Later slice/completion replication is
asynchronous, so a replacement may recompute work whose result was not replicated.
The GUI deduplicates results by job ID; network callback delivery is not claimed
to be globally exactly once.

See [Election](docs/election.md), [Job distribution](docs/job-distribution.md),
[Failover](docs/failover.md), and the [migration record](docs/grpc-module-migration.md).

## Tests and demonstrations

```text
mvn verify
mvn "-Dtest=GrpcOnlyClusterTest" test
mvn "-Dtest=GrpcControlRegressionTest,ReplicationAdmissionTest" test
```

Tests live in `integration-tests/src/test/java`. The process-level cluster test
starts actual worker JVMs, joins them through gRPC, kills the coordinator during
a job, and checks the recovered answer and completed progress. Other tests cover
stream reconnects, worker failure, late joins, stale results, duplicate submissions,
term rotation, topology repair, checkpoint ordering, acceptance failures,
minority-partition quorum safety, leader step-down and the absence of RMI imports
or supertypes.

For a manual demonstration, run bootstrap, four workers and a client. Submit a
PRIMESUM range large enough to stay active, then terminate either a participating
worker or the coordinator. Observe reassignment/election in the event feed, the
reconnecting progress breakdown and the final result. Restart the departed worker
to demonstrate a late join. Bootstrap IDs are not reused until bootstrap restarts;
restart all workers when resetting bootstrap.

## Troubleshooting

- **`release version 25 not supported`:** Maven is using an older JDK.
- **Missing generated protobuf classes:** build the root project, then refresh the
  IDE's Maven projects. Generated sources belong to `shared`.
- **`NoClassDefFoundError: io/grpc/...`:** use the module classpath including
  `target/dependency/*`, after copying dependencies.
- **`Address already in use`:** choose another bootstrap or client callback port.
- **Submission reports unconfirmed acceptance:** start or restore a second worker
  with a reachable failover service, then retry the same submission.
- **No coordinator after losing workers:** leadership requires a majority of the
  ready workers. Restore enough workers to form a majority (for example, three of
  four); a two-of-four remnant remains safely unavailable.
- **Worker exits immediately:** its stdin reached EOF. Run it in its own terminal.
- **Second client receives no results:** give it its own callback port.

## AI assistance

Changes and tests were developed with AI assistance. See the
[historical audit](docs/ai-assisted-audit.md) and
[migration record](docs/grpc-module-migration.md) for disclosure.
