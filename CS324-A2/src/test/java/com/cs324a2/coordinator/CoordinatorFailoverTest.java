package com.cs324a2.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.cs324a2.common.JobRequest;
import com.cs324a2.common.WorkResult;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.scheduler.JobExecutionException;
import com.cs324a2.scheduler.SchedulerConfig;
import com.cs324a2.scheduler.WorkerMembership;
import com.cs324a2.scheduler.WorkerRpcClient;
import com.cs324a2.worker.WorkUnitProcessor;
import com.cs324a2.worker.WorkerProtoMapper;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Failover demonstration over real gRPC coordinator RPCs:
 *
 * <ol>
 *   <li>Three coordinator nodes elect a leader via {@code RequestVote}.</li>
 *   <li>A job is submitted; checkpoints (jobs, task states, results, progress
 *       event numbers) replicate to followers while it runs.</li>
 *   <li>The leader is killed mid-job; a replacement is elected, restores the
 *       replicated state, and resumes only the unfinished tasks.</li>
 *   <li>A later job submitted after recovery is accepted and computed.</li>
 * </ol>
 */
class CoordinatorFailoverTest {
    private final List<CoordinatorNode> nodes = new ArrayList<>();
    private final ExecutorService clients = Executors.newCachedThreadPool(task -> {
        Thread thread = new Thread(task, "failover-client");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void tearDown() {
        clients.shutdownNow();
        for (CoordinatorNode node : nodes) {
            try {
                node.close();
            } catch (RuntimeException ignored) {
                // Best-effort test cleanup.
            }
        }
    }

    @Test
    void killCoordinatorDuringJobElectsReplacementRestoresStateAndAcceptsLaterJobs()
            throws Exception {
        int[] ports = {freePort(), freePort(), freePort()};
        List<WorkerEndpoint> endpoints = List.of(
                endpoint(1, ports[0]), endpoint(2, ports[1]), endpoint(3, ports[2]));
        StaticMembership membership = new StaticMembership(endpoints);
        BlockingRpcClient rpc = new BlockingRpcClient();
        SchedulerConfig config = new SchedulerConfig(Duration.ofSeconds(8),
                Duration.ofSeconds(60), Duration.ofMillis(100), 5, 8, 128);

        CoordinatorNode first = start(1, ports[0], membership, rpc, config);
        CoordinatorNode second = start(2, ports[1], membership, rpc, config);
        CoordinatorNode third = start(3, ports[2], membership, rpc, config);

        // 1. A replacement can only be shown after an initial leader exists.
        first.initiateElection();
        CoordinatorNode leader = awaitSingleLeader(
                List.of(first, second, third), 20_000);
        System.out.printf("[Failover] initial leader=%d term=%d progress=%d%n",
                leader.workerId(), leader.getCurrentTerm(), leader.getProgress());

        // 2. Submit a job that blocks on partition 0 so the kill lands mid-job.
        JobRequest job = JobRequest.max("failover-job", List.of(3, 7, 1, 9, 2, 8));
        var doomed = CompletableFuture.supplyAsync(() ->
                leader.handleSubmit(CoordinatorJobMapper.toSpec(job)), clients);

        awaitCondition(() -> leader.job("failover-job") != null, 10_000,
                "leader admits failover-job");
        assertTrue(rpc.partitionZeroEntered.await(10, TimeUnit.SECONDS),
                "leader should block dispatching partition 0");

        // 3. Followers already hold replicated checkpoints before the kill.
        ReplicatedJobStore.JobRecord replica = awaitReplica(
                followersOf(List.of(first, second, third), leader), "failover-job");
        long completedBeforeKill = replica.tasks().stream()
                .filter(t -> t.status() == ReplicatedJobStore.TaskState.COMPLETED).count();
        System.out.printf("[Failover] replica on follower before kill: tasks=%d completed=%d"
                        + " progress=%d%n",
                replica.tasks().size(), completedBeforeKill, replica.progressIndex());
        assertTrue(replica.progressIndex() > 0);
        long progressAtKill = replica.progressIndex();
        long termAtKill = leader.getCurrentTerm();
        int deadId = leader.workerId();

        // 4. Kill the coordinator mid-job; the old RPC must tell the client
        // to retry rather than report a permanent job failure.
        leader.close();
        rpc.markDead(deadId);
        System.out.printf("[Failover] killed coordinator %d during job %s%n",
                deadId, job.jobId());
        assertFalse(doomed.get(10, TimeUnit.SECONDS).getExecuted(),
                "a shutdown coordinator must return a retryable response");

        // 5. A replacement is elected with a higher term; only one leader remains.
        List<CoordinatorNode> survivors = followersOf(
                List.of(first, second, third), leader);
        CoordinatorNode replacement = awaitSingleLeader(survivors, 20_000);
        assertNotEquals(deadId, replacement.workerId());
        assertTrue(replacement.getCurrentTerm() > termAtKill,
                "replacement term should advance past " + termAtKill);
        System.out.printf("[Failover] replacement elected=%d term=%d (was %d)%n",
                replacement.workerId(), replacement.getCurrentTerm(), termAtKill);

        // 6. State is restored and unfinished work resumes to the right answer.
        ReplicatedJobStore.JobRecord resumed = awaitJobCompleted(replacement,
                "failover-job", 25_000);
        assertEquals(9L, resumed.resultValue());
        assertTrue(resumed.progressIndex() > progressAtKill,
                "progress event numbers should advance past the kill");
        assertEquals(3, resumed.tasks().size());
        assertTrue(resumed.tasks().stream().allMatch(
                t -> t.status() == ReplicatedJobStore.TaskState.COMPLETED));
        System.out.printf("[Failover] work resumed and finished: result=%d progress=%d%n",
                resumed.resultValue(), resumed.progressIndex());

        // 7. A later job submitted after recovery is accepted.
        CoordinatorNode follower = survivors.stream()
                .filter(node -> node != replacement).findFirst().orElseThrow();
        long later = follower.submitJob(JobRequest.primeSum("after-recovery", 1, 20));
        assertEquals(77L, later);
        System.out.printf("[Failover] later job accepted via worker %d: result=%d%n",
                follower.workerId(), later);

        assertSingleLeader(survivors);
    }

    // ---- Helpers ----

    private CoordinatorNode start(int workerId, int port, WorkerMembership membership,
                                  WorkerRpcClient rpc, SchedulerConfig config)
            throws Exception {
        CoordinatorNode node = new CoordinatorNode(workerId, "localhost", port,
                membership, rpc, config);
        nodes.add(node);
        node.start();
        return node;
    }

    private static List<CoordinatorNode> followersOf(List<CoordinatorNode> all,
                                                     CoordinatorNode leader) {
        return all.stream().filter(node -> node != leader).toList();
    }

    private static CoordinatorNode awaitSingleLeader(List<CoordinatorNode> candidates,
                                                     long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<CoordinatorNode> leaders = candidates.stream()
                    .filter(CoordinatorNode::isLeader).toList();
            if (leaders.size() == 1) {
                assertSingleLeader(candidates);
                return leaders.get(0);
            }
            Thread.sleep(50);
        }
        fail("no single leader emerged among "
                + candidates.stream().map(CoordinatorNode::workerId).toList());
        throw new IllegalStateException("unreachable");
    }

    private static void assertSingleLeader(List<CoordinatorNode> candidates) {
        long leaders = candidates.stream().filter(CoordinatorNode::isLeader).count();
        assertEquals(1, leaders, "exactly one active coordinator is allowed, found "
                + leaders);
    }

    private static ReplicatedJobStore.JobRecord awaitReplica(
            List<CoordinatorNode> followers, String jobId) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (CoordinatorNode follower : followers) {
                ReplicatedJobStore.JobRecord replica = follower.job(jobId);
                if (replica != null && replica.tasks().stream().anyMatch(
                        t -> t.status() == ReplicatedJobStore.TaskState.COMPLETED)) {
                    return replica;
                }
            }
            Thread.sleep(50);
        }
        fail("no follower replicated a completed task for " + jobId);
        throw new IllegalStateException("unreachable");
    }

    private static ReplicatedJobStore.JobRecord awaitJobCompleted(CoordinatorNode node,
                                                                  String jobId,
                                                                  long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            ReplicatedJobStore.JobRecord record = node.job(jobId);
            if (record != null
                    && record.status() == ReplicatedJobStore.JobStatus.COMPLETED) {
                return record;
            }
            Thread.sleep(50);
        }
        fail("job " + jobId + " was not restored and completed on worker "
                + node.workerId());
        throw new IllegalStateException("unreachable");
    }

    private static void awaitCondition(java.util.function.BooleanSupplier check,
                                       long timeoutMillis,
                                       String description) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.getAsBoolean()) return;
            Thread.sleep(25);
        }
        fail("timed out waiting: " + description);
    }

    private static WorkerEndpoint endpoint(int workerId, int port) {
        return WorkerEndpoint.newBuilder().setWorkerId(workerId).setHost("localhost")
                .setPort(port).setRegistrationId("failover-reg-" + workerId).build();
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
                if (version == observedVersion) {
                    changed.awaitNanos(duration.toNanos());
                }
                return version;
            } finally {
                lock.unlock();
            }
        }

        @Override public void close() {}
    }

    /** Computes for real, but blocks the first attempt of partition 0 on demand. */
    private static final class BlockingRpcClient implements WorkerRpcClient {
        private final WorkUnitProcessor processor = new WorkUnitProcessor();
        private final CountDownLatch partitionZeroEntered = new CountDownLatch(1);
        private final CountDownLatch releasePartitionZero = new CountDownLatch(1);
        private final Set<Integer> deadWorkers = ConcurrentHashMap.newKeySet();
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final AtomicBoolean released = new AtomicBoolean();

        private void markDead(int workerId) {
            deadWorkers.add(workerId);
            if (released.compareAndSet(false, true)) releasePartitionZero.countDown();
        }

        @Override public ExecuteTaskResponse execute(WorkerEndpoint worker,
                                                     ExecuteTaskRequest request,
                                                     Duration timeout) {
            calls.add(worker.getWorkerId() + ":" + request.getTaskId()
                    + ":attempt" + request.getAttempt());
            if (deadWorkers.contains(worker.getWorkerId())) {
                throw Status.UNAVAILABLE.withDescription(
                        "worker " + worker.getWorkerId() + " is dead").asRuntimeException();
            }
            if (request.getJobId().equals("failover-job")
                    && request.getPartitionId() == 0
                    && request.getAttempt() == 1) {
                partitionZeroEntered.countDown();
                try {
                    if (!releasePartitionZero.await(20, TimeUnit.SECONDS)) {
                        throw Status.DEADLINE_EXCEEDED.withDescription(
                                "partition 0 was never released").asRuntimeException();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Status.CANCELLED.withDescription(
                            "leader died while partition 0 was blocked")
                            .withCause(e).asRuntimeException();
                }
            }
            WorkResult result = processor.execute(
                    WorkerProtoMapper.toWorkUnit(request));
            return WorkerProtoMapper.toResponse(request, worker.getWorkerId(), result);
        }

        @Override public void retainWorkers(long version,
                                            Collection<WorkerEndpoint> workers) {}

        @Override public void close() {}
    }
}
