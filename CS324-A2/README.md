# CS324-A2

Java RMI cluster that runs distributed jobs across worker nodes under an elected coordinator.

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

Workers should heartbeat every 2 seconds. Bootstrap expires a worker after 8 seconds without a heartbeat. `WatchMembership` sends a fresh snapshot when a client connects, followed by live join and leave events. Worker startup will be switched to this Bootstrap service as the worker gRPC service is integrated.

## Run current Assignment 1 cluster

The worker and client launch commands below still use the existing RMI implementation. Use a separate terminal for each process and run each command from the `CS324-A2` directory. These commands will be migrated as the other gRPC services are integrated.

## Run a gRPC computation worker

Start the Bootstrap gRPC server first, then start each worker with a unique ID,
an address reachable by the other cluster machines, and a unique listening port:

```bat
mvn exec:java -Dexec.mainClass=com.cs324a2.worker.WorkerServer -Dexec.args="1 192.168.1.20 51001 localhost 50051"
```

The worker starts its task server before registering with Bootstrap and stops
accepting tasks before deregistering during graceful shutdown.

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
