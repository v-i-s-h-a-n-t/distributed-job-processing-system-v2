# CS324-A2 DistriLab

Distributed computing cluster using gRPC and Protocol Buffers for all
inter-process communication. Worker processes form an unstructured network,
elect a coordinator, accept jobs from multiple clients, and split work across
available workers using Java threads.

## Requirements

Java 17, Maven 3.9+

## Build

```bash
cd CS324-A2
mvn package -DskipTests
```

Run processes with `mvn exec:java`. A bare `java -cp target/classes` fails with
`NoClassDefFoundError` because the gRPC and protobuf jars are not on that
classpath.

## Running the cluster

Each command runs in its own terminal, from `CS324-A2`.

Bootstrap, the membership directory:

```bash
mvn exec:java -Dexec.mainClass=com.cs324a2.bootstrap.BootstrapServer -Dexec.args="50051"
```

Worker and coordinator nodes. Arguments are
`<workerId> <advertisedHost> <workerPort> <bootstrapHost> <bootstrapPort>`.

```bash
mvn exec:java -Dexec.mainClass=com.cs324a2.coordinator.CoordinatorServer -Dexec.args="1 localhost 51001 localhost 50051"
mvn exec:java -Dexec.mainClass=com.cs324a2.coordinator.CoordinatorServer -Dexec.args="2 localhost 51002 localhost 50051"
mvn exec:java -Dexec.mainClass=com.cs324a2.coordinator.CoordinatorServer -Dexec.args="3 localhost 51003 localhost 50051"
```

Client GUI:

```bash
mvn exec:java -Dexec.mainClass=com.cs324a2.client.ClientApp
```

One node logs `*** BECAME COORDINATOR *** term=N`.

On multiple machines, replace `localhost` in each worker's advertised host with
its reachable IP address.

> Election-capable worker = `CoordinatorServer`. It runs task execution plus
> the JAC election and job dispatch. `worker.WorkerServer` is task-only: it
> executes subtasks but never stands for election (no JAC, no
> `CoordinatorService`).

## Services

`bootstrap.proto` tracks live workers with `RegisterWorker`, `Heartbeat`,
`DeregisterWorker`, `ListWorkers` (= suggested `GetActiveWorkers`),
`GetRandomWorker` (uniform draw for unstructured joins), and the
`WatchMembership` stream. Workers heartbeat every 2 seconds and are dropped
after 8 seconds of silence or 2 consecutive reported task transport failures.

`worker.proto` exposes `ExecuteTask`, which runs one partition, plus `Ping`
(liveness probe, suggested `Heartbeat`/`Ping`).

`coordinator.proto` covers election and dispatch: `AddNeighbor`
(= suggested `Join`/`AddNeighbour`), `PropagateElection` (= `Election`),
`PropagateCoordinator` (= `Coordinator`), `Ping`, `RequestVote` (legacy,
retained), `LeaderHeartbeat`, `ReplicateJobState` (= suggested
`ReplicateState`), `SubmitJob` (= `SubmitJob`), `GetCoordinatorState`
(= `FetchState`/`GetResult`), and `SubscribeJobProgress`
(= `StreamProgress`, server streaming).

Suggested-§5 → actual mapping:

| Suggested | Actual |
|---|---|
| `BootstrapService/Register`, `Deregister`, `GetActiveWorkers`, `GetRandomWorker` | `RegisterWorker`, `DeregisterWorker`, `GetActiveWorkers` (= `ListWorkers`), `GetRandomWorker` |
| `ElectionService/Election`, `Coordinator` | `CoordinatorService/PropagateElection`, `PropagateCoordinator` |
| `WorkerService/Join`/`AddNeighbour`, `Heartbeat`/`Ping`, `ExecuteSubtask` | `AddNeighbor`, `Ping` (both services), `ExecuteTask` |
| `JobService/SubmitJob`, `StreamProgress`, `GetResult` | `SubmitJob`, `SubscribeJobProgress`, `GetCoordinatorState` + submit response |
| `StateService/ReplicateState`, `FetchState` | `ReplicateJobState`, `GetCoordinatorState` |

## Client GUI

Set Host and Port in the CONNECTION section. Defaults are `localhost` and
`50051`, the gRPC Bootstrap. The header shows the live worker count.

Operations are `MAX`, `PRIMECOUNT`, and `PRIMESUM`. Enter values directly or use
Load CSV. Multiple jobs run at once.
Number-list CSVs such as `data/numbers-xl.csv` are for `MAX` or `PRIMECOUNT`;
if one is loaded while `PRIMESUM` is selected, the GUI offers those two
operations. A `PRIMESUM` CSV contains one `start,end` row.

### Progress

The bar shows `completedTasks / totalTasks` taken from real task checkpoints,
streamed over gRPC with `SubscribeJobProgress`. It does not advance on a timer.
Range jobs are split into 4 sub-ranges per worker, capped at 32 tasks per job, so
long sieves report progress steadily.

Click a bar for the worker breakdown: per-worker counts plus per-task detail
(partition, status, worker, attempt, result). It refreshes while the job runs
and keeps working across a coordinator failover.

Clear finished removes DONE and FAILED rows. Running jobs keep their streams.

### Input limits

`MAX` and `PRIMECOUNT` take 32-bit integers. `PRIMECOUNT` may be empty and
returns 0. `PRIMESUM` takes an inclusive 64-bit range, read and validated in
64-bit space with no silent rounding or clamping.

The span `end - start + 1` is capped at 100,000,000 values
(`JobRequest.MAX_RANGE_SPAN`), checked on typed, CSV, and submitted input alike.
The sieve handles endpoints up to 2,500,000,000,000,000. Sums that overflow a
64-bit result fail with an error.

## Failure handling

### Workers

Bootstrap drops a worker after 8 seconds without a heartbeat or 2 consecutive
task transport failures for that registration, and broadcasts the removal.
Each coordinator node also watches the membership stream and prunes
departed workers from its neighbour list, and drops a neighbour after 3
consecutive election/heartbeat RPC failures — so floods stop dialling dead
peers while the job continues. A retry avoids its previous worker when another
worker is available. Failed tasks are retried up to
`SchedulerConfig.maximumAttempts`. Work left RUNNING by a dead worker returns to
PENDING and is reassigned. New workers can join mid-job (register + link to a
random active worker).

Detection values (chosen for a lab LAN; all configurable): leader heartbeat
150 ms, follower election timeout 500–1000 ms randomised (distinguishes a slow
worker, which still heartbeats in time, from a failed one, which misses the
whole window); Bootstrap heartbeat 2 s, expiry 8 s (≈4 missed heartbeats:
suspect after ~2 misses, confirmed failed at 4); neighbour link dropped after 3
consecutive RPC failures.

### Coordinator

Election is the Assignment-1 unstructured JAC flood over gRPC:

- Workers form an unstructured overlay (each node links to a subset of peers;
  a joining node links to one random active worker, bilaterally).
- The first submitted job starts an election when no coordinator exists.
  After a coordinator has served, a heartbeat timeout can also start an
  election to recover from failure. Any worker may initiate: an
  `ELECTION` message floods neighbours-except-sender, duplicates are ignored
  via `seenElectionIds`, and each subtree convergecasts its best candidate, so
  all reachable active workers are considered.
- Winner = highest replicated progress; among equally current replicas, the
  lowest Job Allocation Counter (JAC) wins, with ties going to the highest
  worker ID.
- The winner is announced with a `COORDINATOR` flood (`seenCoordinatorIds`
  dedup) so every reachable worker installs the same leader; stale terms are
  rejected and same-term conflicts merge deterministically to the more current
  replica, then the better JAC
  candidate, giving one coordinator per term.
- One term serves at most 5 jobs (`MAX_JOBS_PER_TERM`): admissions increment
  JAC + term count; the 5th admission closes the term; once in-flight jobs
  drain, the leader steps down and a new election runs. A term also ends early
  if the coordinator fails (heartbeat timeout → new election).
- JAC durability (§7.3): each node owns its counter (survives on the node;
  elections read live values through the flood, so no stale copy can win);
  the leader's JAC rides on every heartbeat and all of
  JAC / jobs-this-term / leader JAC / neighbours are exposed via
  `GetCoordinatorState`. Job state itself (term, specs, subtask assignments,
  attempts, completed results, progress numbers, job IDs) is replicated to
  followers on every mutation with `ReplicateJobState`.
- Split-brain protection: monotonic terms, only the first/best leader per term
  is recognised, job admission needs a majority heartbeat lease, leaders
  recheck leadership mid-job and abort on loss.
- A new leader pulls available peer snapshots before serving, reclaims RUNNING
  tasks, keeps COMPLETED
  results, and resumes unfinished jobs without client help.

Demo: submit a wide PRIMESUM and kill the leader mid-job. The survivors elect a
new leader and log `event=RECOVER`, `event=RESUME`, then `event=JOB_DONE`. The
GUI bar keeps advancing because it follows replicated state.

## Tests

```bash
mvn test
```

Tests cover the gRPC membership, scheduling, progress, client, and coordinator
recovery paths. Targeted runs:

```bash
mvn -Dtest=BootstrapGrpcServiceTest test    # membership, leases, watch stream, random draw
mvn -Dtest=CoordinatorElectionTest test     # terms, votes, split-brain (legacy handlers)
mvn -Dtest=JacElectionTest test             # JAC rule, dedup, agreement, pruning, 5-job rotation
mvn -Dtest=CoordinatorFailoverTest test     # kill the leader mid-job
mvn -Dtest=CoordinatorProgressTest test     # progress streaming
mvn -Dtest=GrpcClientServiceTest test       # discovery, submit, progress
```

## Authors

- Vishant Kumar: S11230430
- Anav Chand: S11221203
- James Kado: S11200776
- Rohan Nandan: S11234883
