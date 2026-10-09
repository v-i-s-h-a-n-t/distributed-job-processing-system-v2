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

## Authors

- Vishant Kumar: S11230430
- Anav Chand: S11221203
- James Kado: S11200776
- Rohan Nandan: S11234883
