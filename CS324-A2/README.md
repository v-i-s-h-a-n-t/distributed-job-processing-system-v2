# CS324-A2

This project contains the original Assignment 1 RMI cluster and the Assignment 2 gRPC Bootstrap and worker services. The gRPC worker path can register with Bootstrap, while the legacy client and coordinator flow still uses RMI.

## Prerequisites

- Java 17
- Maven 3.9+

## Build

Run these commands from the `CS324-A2` directory (the directory containing this `pom.xml`).

```bat
mvn package -DskipTests
```

## Bootstrap gRPC service

Generate the Protocol Buffer and gRPC Java classes and compile the project from `CS324-A2`:

```bat
mvn package -DskipTests
```

Start the Bootstrap gRPC membership service on port `50051`:

```bat
mvn exec:java -Dexec.mainClass=com.cs324a2.bootstrap.BootstrapServer -Dexec.args="50051"
```

The gRPC worker sends a heartbeat every 2 seconds. Bootstrap expires a worker after 8 seconds without a heartbeat. `WatchMembership` sends a fresh snapshot when a client connects, followed by live join and leave events.

## Coordinator election and job-state recovery (gRPC)

All election RPCs travel over gRPC (`CoordinatorService` in
`src/main/proto/coordinator.proto`), replacing the legacy RMI
`propagateElection / propagateCoordinator` path for the gRPC cluster:

- **Leader terms** — every vote, heartbeat, replicate, and submit carries a
  monotonic term; stale terms are rejected and higher terms force step-down.
- **Voting** — one vote per term; a candidate needs a majority of the
  Bootstrap membership and must be at least as up-to-date (replicated progress
  watermark) as the voter, so the winner can restore state.
- **Leader heartbeats** — every 150 ms; followers start an election after a
  randomized 500–1000 ms timeout without a valid heartbeat.
- **Two-coordinator protection** — single vote per term, only the first leader
  per term is recognised (a second claimant in the same term is rejected),
  votes are denied while a live leader holds the term, and job admission
  requires a majority heartbeat lease; leaders re-check leadership while a job
  runs and abort on loss.
- **Replication / checkpointing** — every mutation (admit, assign, complete,
  retry, job done) takes the next global progress event number and is pushed
  to followers via `ReplicateJobState` (job spec, per-task states, results,
  progress numbers). Followers keep the newest snapshot per job.
- **Recovery** — a newly elected leader reclaims in-flight (`RUNNING`) tasks
  to `PENDING`, keeps `COMPLETED` results, and resumes unfinished jobs in the
  background without client help.

Run three combined worker+coordinator nodes (after starting Bootstrap on
`50051`):

```bat
mvn exec:java -Dexec.mainClass=com.cs324a2.coordinator.CoordinatorServer -Dexec.args="1 localhost 51001 localhost 50051"
mvn exec:java -Dexec.mainClass=com.cs324a2.coordinator.CoordinatorServer -Dexec.args="2 localhost 51002 localhost 50051"
mvn exec:java -Dexec.mainClass=com.cs324a2.coordinator.CoordinatorServer -Dexec.args="3 localhost 51003 localhost 50051"
```

One node logs `*** ELECTED LEADER term=N ***` and followers accept its
heartbeats. Submit a job to any node (followers forward over gRPC); watch
`event=ADMIT/ASSIGN/COMPLETE ... progress=N` lines replicate.

Kill-the-coordinator demo: submit a job, then kill the leader process
mid-job. The survivors log a new `*** ELECTED LEADER term=N+1 ***`,
`event=RECOVER` with the restored completed-task count, `event=RESUME`
reclaiming in-flight tasks, then `event=COMPLETE`/`JOB_DONE` for the resumed
work. Later jobs submitted to any survivor are accepted. The automated
version of this demo is `CoordinatorFailoverTest`:

```bat
mvn -Dtest=CoordinatorFailoverTest test
```

## Run a gRPC computation worker

Start the Bootstrap gRPC server first. Then start each worker with a unique ID,
a unique listening port, and an advertised host address reachable by the other
cluster machines. When all processes run on one computer, use `localhost` as
the advertised host:

```bat
mvn exec:java -Dexec.mainClass=com.cs324a2.worker.WorkerServer -Dexec.args="1 localhost 51001 localhost 50051"
mvn exec:java -Dexec.mainClass=com.cs324a2.worker.WorkerServer -Dexec.args="2 localhost 51002 localhost 50051"
mvn exec:java -Dexec.mainClass=com.cs324a2.worker.WorkerServer -Dexec.args="3 localhost 51003 localhost 50051"
```

Each worker starts its task server before registering with Bootstrap. On
graceful shutdown, it stops accepting tasks and deregisters. For workers on
different computers, replace `localhost` in the advertised-host argument with
the worker computer's reachable IP address. The Bootstrap host argument must
also be reachable from each worker.

## Run the legacy Assignment 1 RMI cluster

The commands in this section run the original RMI implementation. They use a
separate RMI Bootstrap on port `1099`; they do not use the gRPC Bootstrap on
port `50051`. Run each command from the `CS324-A2` directory, with a separate
terminal for each process.

1. Bootstrap node (directory service, default port 1099):

```bat
java -cp target/classes com.cs324a2.bootstrap.BootstrapNode 1099
```

2. Workers (each needs a unique integer ID):

```bat
java -cp target/classes com.cs324a2.worker.WorkerNode 1 localhost 1099
java -cp target/classes com.cs324a2.worker.WorkerNode 2 localhost 1099
java -cp target/classes com.cs324a2.worker.WorkerNode 3 localhost 1099
```

New workers register with the bootstrap and connect to one random active worker. Leave them running.

3. Client GUI (run one or more instances):

```bat
java -cp target/classes com.cs324a2.client.ClientApp
```

## Using the client

- Set Host, Port, and Client ID in the CONNECTION section. The header shows live worker count.
- In NEW JOB, pick an operation:
  - `MAX` — largest value in a list
  - `PRIMECOUNT` — count of primes in a list
  - `PRIMESUM` — sum of primes in a range
- Enter data manually (`2, 4, 5, 11` or `start` / `end`) or use Load CSV.
- Submit Job. Multiple jobs can run concurrently.

## CSV format

List jobs (`MAX`, `PRIMECOUNT`): single column with optional header, one integer per line:

```csv
value
83811
14593
3279
```

Range jobs (`PRIMESUM`): single row with optional header. Endpoints accept 64-bit integers. The sieve supports endpoints up to 2,500,000,000,000,000; very wide ranges can still take a long time, and a sum too large for a 64-bit result fails with an overflow error. The calculation uses a segmented sieve:

```csv
start,end
1,1000
```

Sample files in `data/`:

- `numbers-sm.csv` with 100 entries
- `numbers-md.csv` with 500 entries
- `numbers-lg.csv` with 1000 entries

## Tests

```bat
mvn test
```

Run only the Bootstrap membership tests:

```bat
mvn -Dtest=BootstrapGrpcServiceTest test
```

Run only the coordinator election and failover tests:

```bat
mvn -Dtest='CoordinatorElectionTest,CoordinatorFailoverTest' test
```

## Authors

- Vishant Kumar: S11230430
- Anav Chand: S11221203
- James Kado: S11200776
- Rohan Nandan: S11234883
