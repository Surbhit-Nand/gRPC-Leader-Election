# CS324 Assignment 2 (2026) - Distributed Worker Cluster

The gRPC computation feature and integration instructions are in
[Job distribution](docs/job-distribution.md). The run instructions below still
launch the existing application; its GUI submission, discovery and election
paths have not yet been migrated fully from RMI.

## Prerequisites

- **JDK 25 (or newer)** — the project targets Java 25 (latest LTS). You must have JDK 25+ installed or the build will fail with a "release version not supported" error.
  - Check your version: `java -version`
  - If you have an older JDK, install JDK 25 (e.g. from [Adoptium](https://adoptium.net/)) and make sure it is selected in your IDE / `JAVA_HOME`.
- **Maven 3.9+** — used to build and run the project.

## First-Time Setup

Clone the repo:

```
git clone https://github.com/Surbhit-Nand/gRPC-Leader-Election.git
```

Move into the project folder:

```
cd gRPC-Leader-Election
```

Build the project:

```
mvn compile dependency:copy-dependencies -DincludeScope=runtime
```

To run the Bootstrap Node, the Workers and the Client, see [How to Run](#how-to-run-any-ide-or-terminal) below.

---

## How to Run (any IDE or terminal)

The project uses gRPC runtime dependencies. The build command above copies them to `target/dependency`. The quickest way to run anything below is the helper script (one terminal per node, project root) — it carries the right classpath automatically:

```
.\run.ps1 build                  # compile + fetch the jars (run once first)
.\run.ps1 bootstrap              # Bootstrap Node (start first, one only)
.\run.ps1 worker                  # Worker Node (repeat per terminal)
.\run.ps1 client                  # Client GUI (repeat per terminal, see ports below)
```

The equivalent manual `java` commands follow for any IDE or terminal; they behave the same in VS Code, IntelliJ, Eclipse and a plain terminal; Apache NetBeans needs extra setup for the worker, see [Apache NetBeans](#apache-netbeans). The commands below use the Windows classpath separator (`;`); use `:` on Linux/macOS. Every node is a separate process, so start each one in its own terminal.

**All nodes run on the same machine** (`localhost`), one terminal each.

Bootstrap Node (start first, one only). The optional second argument is the
gRPC registry port (default `50051`); RMI keeps the first:

```
java -cp "target/classes;target/dependency/*" com.cs324.bootstrap.BootstrapNode 1099
```

Worker Node (one per terminal, no id/port typing). Bootstrap auto-assigns a monotonic
worker id and the worker picks the first free RMI port from 3500 upward:

```
java -cp "target/classes;target/dependency/*" com.cs324.worker.WorkerNode localhost 1099
java -cp "target/classes;target/dependency/*" com.cs324.worker.WorkerNode localhost 1099
```

Prefix `--grpc` (optionally `--grpc-port N`, default `50051`) to register over
the gRPC `RegistryService` instead of RMI — same ids, same peer handoff.
Neighbour links, heartbeats and jobs still use RMI until their owners migrate.

Each run prints the assigned id, e.g. `Worker 1 bound as Worker1 on port 3500 (auto-assigned)`.
Ids are never reused within one Bootstrap run (a middle id left by a departed worker stays
empty); restarting Bootstrap resets the sequence to 1. Restart all workers together for a fresh run.

**The cluster elects a coordinator by itself** about 3 seconds after the workers start, so there is nothing to trigger by hand. A worker that already knows a coordinator stays quiet. To force a fresh election at any time, press `[1]` in a worker's terminal.

Client (Swing GUI). Every client process needs **its own callback port** for receiving results, so start one client per terminal and give each a different port — `1199`, `1200`, `1201`, ... Launching the same port twice on one machine makes the second client fail to bind:

```
java -cp "target/classes;target/dependency/*" com.cs324.client.ClientApp localhost 1099 1199
java -cp "target/classes;target/dependency/*" com.cs324.client.ClientApp localhost 1099 1200
```

The arguments are `bootstrapHost bootstrapPort clientRmiPort`; all three are optional and default to `localhost 1099 1199`.

> The `java` commands above and `.\run.ps1 bootstrap|worker|client` are interchangeable — the script just fills in the defaults shown here. A bare `java -cp target/classes ...` (without `target/dependency/*`) will fail with `NoClassDefFoundError: io/grpc/...` because the gRPC jars live in `target/dependency`.

### VS Code

Press **F5** (Run and Debug) and pick a configuration from `.vscode/launch.json`:

| Configuration | Main class | Arguments |
| --- | --- | --- |
| `Bootstrap Node (port 1099)` | `com.cs324.bootstrap.BootstrapNode` | `1099` |
| `Worker (auto id+port)` | `com.cs324.worker.WorkerNode` | `localhost 1099` |
| `Client App (callback port 1199)` | `com.cs324.client.ClientApp` | `localhost 1099 1199` |
| `Client App 2 (callback port 1200)` | `com.cs324.client.ClientApp` | `localhost 1099 1200` |

Every configuration names an explicit main class, so the IDE never has to guess it. The two client configurations use different callback ports, which is what lets you run two clients at once.

### Apache NetBeans

NetBeans has no `nbactions.xml` in this repository, so the two shortcuts are **not** equivalent to the other IDEs:

- **Shift+F6** (*Run File*) infers the main class from the file you have open, and passes no arguments. That is fine for `BootstrapNode`, `ClientApp` and now `WorkerNode` too, which all fall back to the correct defaults (`WorkerNode` auto-assigns id + port from 3500 and uses `localhost 1099`).
- **F6** (*Run Project*) always runs the pom's default main class, `com.cs324.client.ClientApp`, with an empty `exec.args` - so it starts a client rather than whichever node you were editing.
- To start a worker, set the arguments explicitly under *Project Properties -> Run*, or better create one run configuration per node via *Run -> Set Main Class -> Customize...*:

  | Main class | Arguments |
  | --- | --- |
  | `com.cs324.bootstrap.BootstrapNode` | `1099` |
  | `com.cs324.worker.WorkerNode` | `localhost 1099` |
  | `com.cs324.client.ClientApp` | `localhost 1099 1199` |

  Start one worker per run configuration; each auto-assigns its own id and port. Use a different callback port for a second client.
- Register a **JDK 25 or newer** under *Tools -> Java Platforms*, because the pom targets `release 25`.
- Once a run configuration is verified on one machine, NetBeans can export it to `nbactions.xml` in the project root so the whole team shares the same setup. This repository does not track one yet, so treat the table above as the source of truth.

### Command line with Maven (optional)

The `client`, `bootstrap` and `worker` profiles only set the default `exec.mainClass` / `exec.args`, so the short form is enough for a quick look at a node:

```
mvn -Pclient exec:java
mvn -Pbootstrap exec:java
mvn -Pworker exec:java
```

For anything that has to keep running, fork a JVM with `exec:exec` (`%classpath` is replaced by Maven with the project classpath):

```
mvn exec:exec -Dexec.executable=java -Dexec.args="-classpath %classpath com.cs324.bootstrap.BootstrapNode 1099"
```

**Why fork:** `exec:java` runs the program inside the Maven JVM and only tracks threads created in its own thread group. RMI (`LocateRegistry.createRegistry`, exported remote objects) creates non-daemon threads in the *system* thread group, so a node started that way can disappear as soon as the build finishes - measured on this repo: `mvn -Pbootstrap exec:java` printed `Bootstrap Node running on port 1099 as BootstrapRegistry` and then exited, while the `exec:exec` form was still listening on port 1099 after 30 seconds and released the port when Maven was stopped. The same applies to the client, which creates its own result registry, so the `java -cp "target/classes;target/dependency/*"` commands above are the ones to use for the demo.

### Troubleshooting

- `Error: Could not find or load main class ${packageClassName}` - the IDE ran NetBeans' default Maven "run" action without substituting `${packageClassName}` (this is what the Oracle Java extension for VS Code did). That placeholder is filled in by the IDE, never by Maven, so it must not reach the command line. Use one of the recipes above instead.
- `release version 25 not supported` - the IDE or Maven is running on a JDK older than 25. Point it at JDK 25+.
- A second client exits immediately, or the first one stops receiving results - the two are sharing a callback port. Give each client its own port (see [How to Run](#how-to-run-any-ide-or-terminal)).
- `Address already in use` on a worker - that RMI port is still held by an earlier run. Pick a different one, or close the old terminal.
- `NoClassDefFoundError: io/grpc/...` at startup - the node was launched with bare `target/classes`. Every node needs the dependency jars: use `.\run.ps1 <node>` or `java -cp "target/classes;target/dependency/*" ...` after `mvn compile dependency:copy-dependencies`.

---

## Running the Tests

```
mvn test
```

160 JUnit 5 tests cover the election protocol, duplicate-message handling, job splitting and aggregation, client callbacks, coordinator rotation, thread safety, auto-assigned worker ids, partition-healing rewire, the cluster-activity monitor, range warnings, gRPC contract shapes, Bootstrap discovery over gRPC, and job progress streaming.

---

## Design Decisions

These are settled interpretations of ambiguous spec points. Keep implementations consistent with these across all worker/election/job-dispatch code.

- **JAC increments per worker assignment, not per client job.** Per the spec: _"The counter is incremented each time that worker, while acting as coordinator, assigns a job to another worker."_ Taken literally, if a single client job is split across 3 other workers, the coordinator's JAC increases by 3 in that one request — not by 1. The JAC's only role is therefore to rank candidates during an election (lowest wins, highest ID breaks ties).
- **The JAC is charged when the assignment is issued, and is not refunded.** A failed RMI hand-off still counts: the coordinator incremented when it decided to assign the share, not when the share arrived. A worker that dies or is unreachable mid-dispatch therefore leaves the JAC slightly higher than the work it actually shed, and it is deliberately not corrected. Undoing the increment would need a rollback path that races other in-flight assignments, and a JAC that is briefly pessimistic is harmless here because it only ranks election candidates. A dead participant's share is still computed locally by the coordinator, so the client's result stays correct regardless of the counter.
- **A term lasts 5 client jobs, counted per job rather than per assignment.** The job budget is tracked separately from the JAC, so a term's length does not depend on how many workers happen to be active: the coordinator accepts five client jobs, then starts a new election and yields. Counting assignments instead would have ended a term after two jobs on a four-worker cluster, which does not match the "five jobs per term" rule. The counter resets whenever a node becomes the coordinator in a newly concluded election, so each term gets a full budget. Only a concluded election starts a new term: a joining worker adopts the coordinator it is told about locally and does not re-announce it, and the node that wins an election records its announcement before broadcasting, so a repeated or adopted announcement cannot hand a serving coordinator a second budget mid-term.
- **The fifth job of a term triggers the handover, not the job's completion.** The budget is spent when a job is accepted, so rotation is driven from `submitJob`; waiting for the fifth job to finish would let further jobs slip into a term that was already full. A term that is full also refuses further jobs with a retryable error instead of queueing them, so "at most five jobs" holds under concurrent submissions, and the refusal is safe for clients because the job id is released and the client retries against the new coordinator. The refusal covers the handover itself: the budget is reset the moment the term ends, but the yielding node is still named coordinator until the election concludes, so it refuses work for that whole window rather than starting a fresh budget inside a term the spec has already spent.
- **The cluster elects a coordinator on its own.** Workers wait a short grace period after registering and start an election only if they still know no coordinator, and a joining worker adopts the leader its peer already knows. No manual step is needed before the first job, and a normal startup converges to a single coordinator.
- **The coordinator participates in every job's computation.** It does not only delegate — it takes its own share of the workload (its own sub-range/sub-list) alongside the workers it assigns tasks to.
- **Coordinator failure detection uses heartbeats.** Workers periodically ping the current coordinator; a missed response is what triggers a new election. This is separate from the election-termination mechanism (echo-based, see election design notes).
- **Fault tolerance is out of scope for this assignment.** No requirement to handle a worker or coordinator dying mid-job or mid-election — that's Assignment 2's focus.
- **Neighbour connections are bidirectional.** When worker A connects to worker B (via a peer the Bootstrap Node introduced), both A and B add each other to their local neighbour lists — a successful RMI call is treated as the acknowledgement, no separate ack message needed.
- **A departing worker never splits the cluster.** Survivors keep a 2-hop view (each neighbour's neighbour set, seeded on connect and refreshed on traffic). On a clean exit the leaver hands its neighbour set over via `notifyLeaving` before deregistering; on a crash the first failed send to the corpse triggers the same path: prune the dead edge and chain to the smallest-id grandparent, falling back to any live Bootstrap peer. One edge per orphan, deterministic pick, so no islands and no cliques.

---

## Election Protocol

See [docs/election.md](docs/election.md) for propagation, duplicate handling, winner rule, coordinator announcement, and 5-job term.

---

## gRPC contracts

`src/main/proto/` holds the frozen service contracts (`package distrilab`, field numbers must not change once merged — `ProtoContractsTest` enforces service and method names):

- `progress.proto` — `JobProgressService.WatchJob`: live per-worker progress streams (already wired into workers and the GUI, see [Job progress](#job-progress-first-a2-migration-step)).
- `registry.proto` — `RegistryService`: Bootstrap discovery (register with auto-assign, deregister, job-id allocation, active-worker and address lookup). Served alongside RMI by `BootstrapNode` (second arg = gRPC port, default `50051`); workers opt in with `--grpc`.
- `election.proto` — `ElectionService` (wave, echo, coordinator announcement) for the election migration; behavior still on RMI.
- `job.proto` — `JobService` (submit, assign, report) plus `ClientService.DeliverResult` for the client callback; payloads are `PrimeRange` messages or `repeated int32` lists. Dispatch still on RMI.

Maven generates the Java stubs during `compile`. Migration status: progress streaming and Bootstrap discovery are dual-stack; election, jobs, and client callbacks remain RMI until their owners migrate.

---

## Packages

- `common` - shared RMI interfaces + constants
- `bootstrap` - Bootstrap Node
- `worker` - Worker nodes (election + job execution)
- `client` - Client GUI

## Updated GUI

The client now has rounded panels, blue buttons, icons, and a job-history table.
It supports manual input, numeric CSV loading, input validation, and clearing
input without removing the submitted-job history. `ClientApp.java` handles the
interface and input; `ClientStyle.java` contains the styling helpers.
`CsvInputLoader.java` holds the parsing, merging and rendering rules so they
can be tested without starting a window.

**Load CSVs** accepts one file or several at once. Several files are read in
filename order, their values are concatenated into a single job, and the merged
values are shown in the input area, wrapped 20 per line. The combined size of
the selected files must stay at or below 1 MB. Parsing stays strict in every
case: bare comma-separated integers only, so quoted fields, headers, empty rows
and missing values are rejected, and the error names the file that caused it.
Note that `PRIMESUM` still needs exactly two values in ascending order across
the whole merged selection, so it is usually a single-file operation.

The GUI validates and submits jobs to the current coordinator: it requests a
cluster-wide job ID from the Bootstrap node, discovers the coordinator through
it, submits MAX/PRIMESUM/PRIMECOUNT jobs (PrimeRange payloads for PRIMESUM), and
fills in the result cell once the coordinator delivers the aggregated answer.
If the coordinator rotates mid-submit it discovers the new one and retries.

### Cluster activity card

Below the job history, the client shows a **Cluster activity** card: a live worker
table (`Worker | Role | Coordinator view | Neighbours | JAC | Status`) and a
timestamped feed of happenings. Each worker keeps a 64-entry ring of its own
events — elections started and announced, coordinators adopted, neighbour edges
added/dropped (with the cause), every job slice dispatched and received, and
clean-exit farewells — exposed via `WorkerRemote.getRecentEvents(seq)`. The
client polls every 2 seconds off the event thread (plus a manual Refresh button,
`Alt+R`), drains only unseen sequences, and falls back to snapshot-diff lines
(joins, leaves, consensus changes) for what workers cannot self-report; a
snapshot heuristic stays silent whenever the exact worker event already tells
that story. Kill a middle worker and watch the exact `dropped … (unreachable)`
+ `chained to …` lines appear, and submit a job to see each slice dispatched and
received per worker.

## Computation

`WorkerTaskCalculator` handles three calculations:

- `MAX`: finds the largest number.
- `PRIMESUM`: adds the prime numbers.
- `PRIMECOUNT`: counts prime numbers, including duplicates.

```java
import com.cs324.worker.WorkerTaskCalculator;
import com.cs324.worker.WorkerTaskCalculator.PrimeRange;

WorkerTaskCalculator.calculate("MAX", new int[]{3, 7, 12, 45}); // 45
WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(1, 10)); // 17
WorkerTaskCalculator.calculate("PRIMECOUNT", new int[]{2, 7}); // list: 2
WorkerTaskCalculator.calculate("PRIMECOUNT", new PrimeRange(2, 7)); // range: 4
```

Arrays (`int[]` or `Integer[]`) and collections of `Integer` always mean lists,
even with only two values. Use `PrimeRange(start, end)` for an inclusive range
in either prime operation. MAX accepts lists only.
MAX and PRIMECOUNT return `Integer`; PRIMESUM returns `Long`.

Invalid inputs, null entries and reversed ranges raise `IllegalArgumentException`.
Empty prime lists return zero; an empty MAX list is rejected. Use 32-bit integers;
decimal values and `long` inputs are rejected rather than silently converted.
Very wide ranges can still be slow.

The calculator is wired into worker execution: `WorkerRemoteImpl` delegates to
`WorkerTaskCalculator` for worker assignments and the coordinator's own chunk.
`WorkerTaskDistributor` supplies the shared chunking and result-combination
logic used by the coordinator's existing callback-based `submitJob()` flow.
The coordinator looks up all registered, reachable workers through bootstrap,
without changing their election-neighbour connections. It registers every expected
partial result before sending chunks, then combines the answers only after dispatch
and computation are complete. Each chunk carries the original coordinator's callback,
so a worker does not need to be its neighbour and a later election does not redirect
an in-progress job's answer. The existing client callback receives the final value.

Run its tests:

```powershell
mvn "-Dtest=WorkerTaskCalculatorTest" test
```

Run the updated client (use the forked form - see [Why fork](#command-line-with-maven-optional)):

```powershell
java -cp "target/classes;target/dependency/*" com.cs324.client.ClientApp localhost 1099 1199
```

### Distribution integration

The client obtains job IDs from `BootstrapRemote.allocateJobId()` so concurrent
clients cannot accidentally reuse IDs. Bootstrap only allocates identifiers and
provides worker addresses; it does not calculate, split or combine jobs. Manual RMI
clients must also request their job IDs from bootstrap. IDs last for one bootstrap
process; restart the whole cluster together when starting a fresh run.

Each client process needs its own callback registry port; see the two-client
commands in [How to Run](#how-to-run-any-ide-or-terminal).

With four registered workers, PRIMESUM(1,1000) splits into 1-250, 251-500,
501-750 and 751-1000 and combines to **76127**. The coordinator takes the first
chunk. Smaller lists/ranges may use fewer workers to avoid empty chunks.

### Wide ranges (up to 1B width)

`PRIMESUM` accepts any ascending `start,end` within the 32-bit int range, and
widths up to a billion submit end-to-end: the splitter uses 64-bit arithmetic
so chunk boundaries stay exact, and sums/counts still fit their result types.
Only wall-clock time stands in the way (trial division to √n per number), so
the client warns before submitting — above 1M width it discloses minutes, above
100M hours — and proceeds on confirm. Nothing is capped: a 1B-wide range runs
for hours across the workers rather than failing.

`ClusterDistributionTest` uses four workers connected in a chain over RMI. It
checks every worker's participation, the assignment's range example, waiting for
a delayed worker, result delivery after a leader change, and two concurrent client
callbacks with distinct job IDs. `WorkerDispatchTest` covers the five-job term:
the coordinator keeps leading through four jobs and rotates to the lowest-JAC
worker on the fifth. Existing election and threading tests remain intact.

## Job progress (first A2 migration step)

Each job has a Progress column. Click its bar to open a live worker breakdown.
The percentage is completed input values divided by total input values, weighted
by the size of each worker's assigned share. For PRIMESUM ranges, each integer in
the inclusive range counts as one value. Updates occur when a whole worker share
returns successfully, so small jobs may go straight to 100%, and one large share
will not show intermediate progress. This is completed work, not elapsed time.
Errors do not count as successful completion.

Progress snapshots use the Protocol Buffers contract in `src/main/proto/progress.proto`
and a gRPC server stream. Each worker opens an available progress port at startup;
the client discovers that port from the coordinator. Maven generates Java protobuf
and service classes during `compile`; reload the Maven project in your IDE.
The coordinator retains the latest 256 completed job snapshots, allowing very fast
jobs to display progress even when they finish before the client subscribes.

This is an incremental migration: job submission, results, elections,
and worker task delivery still use RMI, while progress streaming and Bootstrap
discovery (registration/deregistration) already speak gRPC. Progress updates are at share completion,
not within a share. A locally recovered share keeps its originally assigned worker
label. Interrupted progress streams are displayed explicitly and do not yet reconnect
automatically or recover across coordinator failure. Full A2 transport migration and
failure recovery remain to be implemented.
