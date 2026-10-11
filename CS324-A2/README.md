# CS324-A2 DistriLab

Distributed computing cluster using gRPC and Protocol Buffers for all
inter-process communication. Worker processes form an unstructured network,
elect a coordinator, accept jobs from multiple clients, and split work across
available workers using Java threads.

The original Java RMI implementation is still in the repository for reference
but is not used by the system below.

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

One node logs `*** ELECTED LEADER term=N ***`.

On multiple machines, replace `localhost` in each worker's advertised host with
its reachable IP address. A worker can also run task execution only, with no
election duties:

```bash
mvn exec:java -Dexec.mainClass=com.cs324a2.worker.WorkerServer -Dexec.args="4 localhost 51004 localhost 50051"
```

## Services

`bootstrap.proto` tracks live workers with `RegisterWorker`, `Heartbeat`,
`DeregisterWorker`, `ListWorkers`, and the `WatchMembership` stream. Workers
heartbeat every 2 seconds and are dropped after 8 seconds of silence.

`worker.proto` exposes `ExecuteTask`, which runs one partition.

`coordinator.proto` covers election and dispatch: `RequestVote`,
`LeaderHeartbeat`, `ReplicateJobState`, `SubmitJob`, `GetCoordinatorState`, and
`SubscribeJobProgress`.

## Client GUI

Set Host and Port in the CONNECTION section. Defaults are `localhost` and
`50051`, the gRPC Bootstrap. The header shows the live worker count.

Operations are `MAX`, `PRIMECOUNT`, and `PRIMESUM`. Enter values directly or use
Load CSV. Multiple jobs run at once.

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

Bootstrap drops a worker after 8 seconds without a heartbeat and broadcasts the
removal. Failed tasks are retried elsewhere up to
`SchedulerConfig.maximumAttempts`. Work left RUNNING by a dead worker returns to
PENDING and is reassigned. New workers can join mid-job.

### Coordinator

Election is Raft-style over gRPC:

- Monotonic terms. Stale terms are rejected, higher terms force a step down.
- One vote per term. A candidate needs a majority of the membership and must be
  at least as up to date as the voter, judged by the replicated progress
  watermark.
- Leader heartbeats every 150 ms. Followers start an election after 500 to
  1000 ms without one.
- Split-brain protection: only the first leader per term is recognised, votes
  are denied while a live leader holds the term, and job admission needs a
  majority lease. Leaders recheck leadership mid-job and abort on loss.
- Every mutation (admit, assign, complete, retry, done) takes the next global
  progress number and is replicated to followers with `ReplicateJobState`.
- A new leader restores those snapshots, reclaims RUNNING tasks, keeps COMPLETED
  results, and resumes unfinished jobs without client help.

Demo: submit a wide PRIMESUM and kill the leader mid-job. The survivors elect a
new leader and log `event=RECOVER`, `event=RESUME`, then `event=JOB_DONE`. The
GUI bar keeps advancing because it follows replicated state.

## Tests

```bash
mvn test
```

129 tests across 18 classes. Targeted runs:

```bash
mvn -Dtest=BootstrapGrpcServiceTest test    # membership, leases, watch stream
mvn -Dtest=CoordinatorElectionTest test     # terms, votes, split-brain
mvn -Dtest=CoordinatorFailoverTest test     # kill the leader mid-job
mvn -Dtest=CoordinatorProgressTest test     # progress streaming
mvn -Dtest=GrpcClientServiceTest test       # discovery, submit, progress
```

## Legacy RMI cluster

Assignment 1 code, kept for reference. It uses its own Bootstrap on port 1099
and is not part of the gRPC system. Pointing the GUI at port 1099 runs jobs over
RMI with indeterminate progress bars.

```bash
mvn exec:java -Dexec.mainClass=com.cs324a2.bootstrap.BootstrapNode -Dexec.args="1099"
mvn exec:java -Dexec.mainClass=com.cs324a2.worker.WorkerNode -Dexec.args="1 localhost 1099"
```

## Authors

- Vishant Kumar: S11230430
- Anav Chand: S11221203
- James Kado: S11200776
- Rohan Nandan: S11234883