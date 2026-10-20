package com.cs324a2.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.cs324a2.common.JobRequest;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.coordinator.CoordinatorServiceGrpc;
import com.cs324a2.grpc.coordinator.JobProgressRequest;
import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import com.cs324a2.scheduler.JobExecutionException;
import com.cs324a2.scheduler.SchedulerConfig;
import com.cs324a2.scheduler.GrpcWorkerRpcClient;
import com.cs324a2.scheduler.WorkerMembership;
import com.cs324a2.scheduler.WorkerRpcClient;
import com.cs324a2.worker.WorkUnitProcessor;
import com.cs324a2.worker.WorkerProtoMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the gRPC server-streaming progress contract used by the client GUI:
 * updates are driven by actual task checkpoints, carry a worker breakdown, and
 * are servable from followers (failover path).
 */
class CoordinatorProgressTest {
    private final List<CoordinatorNode> nodes = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final List<GrpcWorkerRpcClient> taskClients = new ArrayList<>();
    private final ExecutorService clients = Executors.newCachedThreadPool(task -> {
        Thread thread = new Thread(task, "progress-test");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void tearDown() {
        clients.shutdownNow();
        for (ManagedChannel channel : channels) {
            try {
                channel.shutdownNow();
            } catch (RuntimeException ignored) {
                // Best effort.
            }
        }
        for (CoordinatorNode node : nodes) {
            try {
                node.close();
            } catch (RuntimeException ignored) {
                // Best-effort test cleanup.
            }
        }
        taskClients.forEach(GrpcWorkerRpcClient::close);
    }

    @Test
    void streamsRealProgressWithWorkerBreakdownAndCompletesOnTerminalState() throws Exception {
        int[] ports = {freePort(), freePort(), freePort()};
        List<WorkerEndpoint> endpoints = List.of(
                endpoint(1, ports[0]), endpoint(2, ports[1]), endpoint(3, ports[2]));
        StaticMembership membership = new StaticMembership(endpoints);
        GrpcWorkerRpcClient rpc = new GrpcWorkerRpcClient();
        SchedulerConfig config = new SchedulerConfig(Duration.ofSeconds(5),
                Duration.ofSeconds(30), Duration.ofMillis(100), 3, 8, 128);

        CoordinatorNode first = start(1, ports[0], membership, rpc, config);
        CoordinatorNode second = start(2, ports[1], membership, rpc, config);
        CoordinatorNode third = start(3, ports[2], membership, rpc, config);

        first.initiateElection();
        CoordinatorNode leader = awaitSingleLeader(List.of(first, second, third), 20_000);
        List<CoordinatorNode> followers = List.of(first, second, third).stream()
                .filter(node -> node != leader).toList();

        // Subscribe from a FOLLOWER before submitting: proves followers serve
        // replicated checkpoints and the stream survives on non-leaders.
        CoordinatorNode follower = followers.get(0);
        List<JobProgressUpdate> followerUpdates = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> followerDone = new CompletableFuture<>();
        clients.execute(() -> {
            try {
                Iterator<JobProgressUpdate> stream = stubFor(
                        followerPort(followers, follower, ports, endpoints))
                        .subscribeJobProgress(JobProgressRequest.newBuilder()
                                .setJobId("progress-job").build());
                while (stream.hasNext()) followerUpdates.add(stream.next());
                followerDone.complete(null);
            } catch (Throwable e) {
                followerDone.completeExceptionally(e);
            }
        });

        // Submit to the leader in the background while the stream is waiting
        // (the server waits for the job to appear, so order is safe).
        CompletableFuture<Long> result = CompletableFuture.supplyAsync(() -> {
            try {
                return leader.submitJob(JobRequest.max("progress-job",
                        List.of(3, 7, 1, 9, 2, 8, 4, 6, 5)));
            } catch (JobExecutionException e) {
                throw new RuntimeException(e);
            }
        }, clients);

        assertEquals(9L, result.get(30, TimeUnit.SECONDS));
        followerDone.get(30, TimeUnit.SECONDS);

        // Fast jobs may coalesce several checkpoint mutations into one
        // observed update (50ms poll), so require at least the terminal update.
        assertTrue(followerUpdates.size() >= 1,
                "expected at least the terminal checkpoint update");
        JobProgressUpdate last = followerUpdates.get(followerUpdates.size() - 1);
        assertEquals("progress-job", last.getJobId());
        assertEquals(last.getTotalTasks(), last.getCompletedTasks());
        assertTrue(last.getTotalTasks() >= 3, "expected >=3 partitions");
        assertEquals(1.0, last.getCompletionFraction(), 1e-9);
        assertTrue(last.getProgressIndex() > 0);
        assertTrue(last.getWorkerBreakdownCount() > 0,
                "worker breakdown must be present for the GUI dialog");
        int breakdownTotal = last.getWorkerBreakdownList().stream()
                .mapToInt(w -> w.getTotalTasks()).sum();
        assertEquals(last.getTotalTasks(), breakdownTotal);
        assertTrue(last.hasJobState());
        assertTrue(last.getJobState().getTasksList().stream().allMatch(
                t -> t.getStatus()
                        == com.cs324a2.grpc.coordinator.TaskStatus.TASK_COMPLETED));

        // Progress numbers advance monotonically and completions never go backwards.
        long previousProgress = -1;
        int previousCompleted = -1;
        for (JobProgressUpdate update : followerUpdates) {
            assertTrue(update.getProgressIndex() > previousProgress,
                    "progress event numbers must increase");
            assertTrue(update.getCompletedTasks() >= previousCompleted,
                    "completed count must not go backwards");
            assertEquals(update.getCompletedTasks() / (double) update.getTotalTasks(),
                    update.getCompletionFraction(), 1e-9);
            previousProgress = update.getProgressIndex();
            previousCompleted = update.getCompletedTasks();
        }
    }

    @Test
    void rangeJobsAreSplitIntoManyTasksSoProgressAdvances() {
        // One task per worker leaves the GUI progress bar at 0% for the whole
        // job, then jumps to 100%: range jobs must fan out into sub-ranges.
        int[] memberCounts = {1, 2, 3, 5, 10};
        for (int members : memberCounts) {
            int partitions = CoordinatorNode.partitionCountFor(
                    JobRequest.primeSum("range-job", 1, 10_000_000), members);
            assertTrue(partitions >= members,
                    "range jobs need at least one task per worker");
            if (members >= 2) {
                assertTrue(partitions > members,
                        "range jobs need more tasks than workers, got " + partitions);
            }
            assertTrue(partitions <= CoordinatorNode.MAX_PARTITIONS_PER_JOB,
                    "task count must stay bounded for replication cost");
            // List jobs keep one partition per worker.
            assertEquals(members, CoordinatorNode.partitionCountFor(
                    JobRequest.max("list-job", List.of(1, 2, 3)), members));
        }
    }

    @Test
    void primeSumContinuesPastFirstChunkAndCheckpointsExactResult() throws Exception {
        int port = freePort();
        StaticMembership membership = new StaticMembership(List.of(endpoint(1, port)));
        CoordinatorNode node = start(1, port, membership, new GrpcWorkerRpcClient(),
                new SchedulerConfig(Duration.ofSeconds(5), Duration.ofSeconds(2),
                        Duration.ofMillis(100), 3, 8, 128));
        node.initiateElection();
        JobRequest request = JobRequest.primeSum("chunked-primes", 1, 400_004);
        long result = node.submitJob(request);
        assertEquals(com.cs324a2.compute.ComputeEngine.primeSum(1, 400_004), result);
        ReplicatedJobStore.JobRecord checkpoint = node.job(request.jobId());
        assertEquals(Long.toString(result), checkpoint.exactResultValue());
        assertTrue(checkpoint.tasks().stream().allMatch(t -> t.attempt() == 1));
        assertEquals(1.0, CoordinatorNode.toProgressUpdate(checkpoint)
                .getCompletionFraction(), 1e-9);

        long start = Long.MAX_VALUE - 2_000;
        JobRequest largeResult = JobRequest.primeSum("exact-primes", start, Long.MAX_VALUE);
        node.submitJob(largeResult);
        String exact = node.job(largeResult.jobId()).exactResultValue();
        assertEquals(com.cs324a2.compute.ComputeEngine.primeSumExact(start, Long.MAX_VALUE)
                .toString(), exact);
        assertTrue(new java.math.BigInteger(exact)
                .compareTo(java.math.BigInteger.valueOf(Long.MAX_VALUE)) > 0);
        assertEquals(exact, node.handleSubmit(CoordinatorJobMapper.toSpec(largeResult))
                .getExactResultValue());
    }

    @Test
    void primeSumShrinksTimedOutChunksAndStillFinishes() throws Exception {
        int port = freePort();
        java.util.concurrent.atomic.AtomicInteger timeouts =
                new java.util.concurrent.atomic.AtomicInteger();
        WorkerRpcClient rpc = new WorkerRpcClient() {
            @Override public com.cs324a2.grpc.worker.ExecuteTaskResponse execute(
                    WorkerEndpoint worker,
                    com.cs324a2.grpc.worker.ExecuteTaskRequest request,
                    Duration timeout) {
                var unit = WorkerProtoMapper.toWorkUnit(request);
                if (unit.end() - unit.start() + 1 > 100) {
                    timeouts.incrementAndGet();
                    throw Status.DEADLINE_EXCEEDED.asRuntimeException();
                }
                return WorkerProtoMapper.toResponse(request, worker.getWorkerId(),
                        new WorkUnitProcessor().execute(unit));
            }
            @Override public void retainWorkers(long version, java.util.Collection<WorkerEndpoint> workers) {}
            @Override public void close() {}
        };
        CoordinatorNode node = start(1, port,
                new StaticMembership(List.of(endpoint(1, port))), rpc,
                new SchedulerConfig(Duration.ofSeconds(1), Duration.ofSeconds(1),
                        Duration.ofMillis(100), 3, 8, 128));
        node.initiateElection();
        assertEquals(com.cs324a2.compute.ComputeEngine.primeSum(1, 4_000),
                node.submitJob(JobRequest.primeSum("adaptive", 1, 4_000)));
        assertTrue(timeouts.get() > 0);
    }

    @Test
    void manyRangeTasksStillProduceCorrectResult() {
        // Guard the granular split end-to-end at the partitioner level.
        var request = JobRequest.primeSum("wide", 1, 1_000_000);
        List<com.cs324a2.common.WorkUnit> units =
                com.cs324a2.compute.WorkPartitioner.partition(request, 12);
        assertEquals(12, units.size());
        long sum = 0;
        long expectedNext = 1;
        for (var unit : units) {
            assertEquals(expectedNext, unit.start(), "partitions must be contiguous");
            expectedNext = unit.end() + 1;
            sum += com.cs324a2.compute.ComputeEngine.primeSum(unit.start(), unit.end());
        }
        assertEquals(1_000_001, expectedNext, "partitions must cover the whole range");
        assertEquals(com.cs324a2.compute.ComputeEngine.primeSum(1, 1_000_000), sum,
                "sub-range sums must add up to the full range");
    }

    @Test
    void progressFractionsComeFromTaskCheckpointsNotTimers() {
        // Deterministic partial-progress check without timing races.
        ReplicatedJobStore store = new ReplicatedJobStore();
        JobRequest request = JobRequest.max("fraction-job", List.of(1, 2, 3));
        List<com.cs324a2.common.WorkUnit> units =
                com.cs324a2.compute.WorkPartitioner.partition(request, 3);
        store.createJob(request, units, 1);
        JobProgressUpdate admitted = CoordinatorNode.toProgressUpdate(store.get("fraction-job"));
        assertEquals(3, admitted.getTotalTasks());
        assertEquals(0, admitted.getCompletedTasks());
        assertEquals(0.0, admitted.getCompletionFraction(), 1e-9);

        // Complete one of three tasks directly in the store.
        ReplicatedJobStore.JobRecord live = store.live("fraction-job");
        synchronized (live) {
            var first = live.tasks().get(0);
            long progress = store.nextProgress();
            live.tasks.put(first.unit().partitionId(),
                    new ReplicatedJobStore.TaskRecord(first.taskId(), first.unit(),
                            ReplicatedJobStore.TaskState.COMPLETED, 1, 2, 3L, null, progress));
            live.progressIndex = progress;
        }
        JobProgressUpdate partial = CoordinatorNode.toProgressUpdate(store.get("fraction-job"));
        assertEquals(1, partial.getCompletedTasks());
        assertEquals(1.0 / 3.0, partial.getCompletionFraction(), 1e-9);
        assertTrue(partial.getWorkerBreakdownCount() > 0);
    }

    @Test
    void rejectsBlankJobId() throws Exception {
        int port = freePort();
        StaticMembership membership = new StaticMembership(List.of(endpoint(1, port)));
        GrpcWorkerRpcClient rpc = new GrpcWorkerRpcClient();
        CoordinatorNode node = start(1, port, membership, rpc, SchedulerConfig.defaults());
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext().build();
        channels.add(channel);
        Iterator<JobProgressUpdate> stream = CoordinatorServiceGrpc.newBlockingStub(channel)
                .subscribeJobProgress(JobProgressRequest.newBuilder().build());
        try {
            stream.hasNext();
            fail("blank job_id should be rejected");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        } finally {
            node.close();
            nodes.remove(node);
        }
    }

    // ---- Helpers ----

    private CoordinatorNode start(int workerId, int port, WorkerMembership membership,
                                  WorkerRpcClient rpc, SchedulerConfig config)
            throws Exception {
        if (rpc instanceof GrpcWorkerRpcClient grpc) taskClients.add(grpc);
        CoordinatorNode node = new CoordinatorNode(workerId, "localhost", port,
                membership, rpc, config);
        nodes.add(node);
        node.start();
        return node;
    }

    private CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stubFor(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext().build();
        channels.add(channel);
        return CoordinatorServiceGrpc.newBlockingStub(channel);
    }

    private static int followerPort(List<CoordinatorNode> followers, CoordinatorNode follower,
                                    int[] ports, List<WorkerEndpoint> endpoints) {
        for (int i = 0; i < endpoints.size(); i++) {
            if (endpoints.get(i).getWorkerId() == follower.workerId()) return ports[i];
        }
        throw new IllegalStateException("unknown follower " + follower.workerId());
    }

    private static CoordinatorNode awaitSingleLeader(List<CoordinatorNode> candidates,
                                                     long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<CoordinatorNode> leaders = candidates.stream()
                    .filter(CoordinatorNode::isLeader).toList();
            if (leaders.size() == 1) return leaders.get(0);
            Thread.sleep(50);
        }
        fail("no single leader emerged");
        throw new IllegalStateException("unreachable");
    }

    private static WorkerEndpoint endpoint(int workerId, int port) {
        return WorkerEndpoint.newBuilder().setWorkerId(workerId).setHost("localhost")
                .setPort(port).setRegistrationId("progress-reg-" + workerId).build();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class StaticMembership implements WorkerMembership {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private final List<WorkerEndpoint> workers;
        private final long version = 1;

        private StaticMembership(List<WorkerEndpoint> workers) {
            this.workers = List.copyOf(workers);
        }

        @Override public Snapshot snapshot() {
            return new Snapshot(version, workers);
        }

        @Override public long awaitChange(long observedVersion, Duration duration)
                throws InterruptedException {
            lock.lockInterruptibly();
            try {
                if (version == observedVersion) changed.awaitNanos(duration.toNanos());
                return version;
            } finally {
                lock.unlock();
            }
        }

        @Override public void close() {}
    }
}
