package com.cs324a2.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a2.common.JobRequest;
import com.cs324a2.common.WorkResult;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.worker.WorkUnitProcessor;
import com.cs324a2.worker.WorkerProtoMapper;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DistributedTaskSchedulerTest {
    private final List<DistributedTaskScheduler> schedulers = new ArrayList<>();

    @AfterEach
    void closeSchedulers() {
        schedulers.forEach(DistributedTaskScheduler::close);
    }

    @Test
    void partitionsAndAggregatesAllOperations() throws Exception {
        MutableMembership membership = new MutableMembership(worker(1), worker(2), worker(3));
        FakeRpcClient rpc = new FakeRpcClient();
        DistributedTaskScheduler scheduler = scheduler(membership, rpc, standardConfig());

        assertEquals(9, scheduler.execute(JobRequest.max(
                "max-job", List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))));
        assertEquals(77, scheduler.execute(JobRequest.primeSum("sum-job", 1, 20)));
        assertEquals(4, scheduler.execute(JobRequest.primeCount(
                "count-job", List.of(2, 2, 4, 5, 11))));
        assertTrue(rpc.calls.stream().allMatch(call -> !call.request().getTaskId().isBlank()));
    }

    @Test
    void retriesUnreachableWorkerOnAnotherWorkerAndIncrementsAttempt() throws Exception {
        MutableMembership membership = new MutableMembership(worker(1), worker(2));
        FakeRpcClient rpc = new FakeRpcClient();
        rpc.failWorkerOnce = 1;
        DistributedTaskScheduler scheduler = scheduler(membership, rpc, standardConfig());

        assertEquals(9, scheduler.execute(JobRequest.max("retry-job", List.of(9, 2))));

        assertTrue(rpc.calls.stream().anyMatch(c -> c.worker().getWorkerId() == 1
                && c.request().getAttempt() == 1));
        assertTrue(rpc.calls.stream().anyMatch(c -> c.worker().getWorkerId() == 2
                && c.request().getAttempt() == 2));
    }

    @Test
    void timeoutReassignsAndLateOldAttemptCannotReplaceResult() throws Exception {
        MutableMembership membership = new MutableMembership(worker(1), worker(2));
        FakeRpcClient rpc = new FakeRpcClient();
        rpc.slowWorker = 1;
        rpc.slowMillis = 180;
        SchedulerConfig config = new SchedulerConfig(Duration.ofMillis(40),
                Duration.ofSeconds(2), Duration.ofMillis(20), 3, 4, 16);
        DistributedTaskScheduler scheduler = scheduler(membership, rpc, config);

        assertEquals(9, scheduler.execute(JobRequest.max("timeout-job", List.of(9))));
        assertTrue(rpc.calls.stream().anyMatch(c -> c.request().getAttempt() == 2
                && c.worker().getWorkerId() == 2));
    }

    @Test
    void departedWorkerAssignmentIsRecovered() throws Exception {
        MutableMembership membership = new MutableMembership(worker(1));
        FakeRpcClient rpc = new FakeRpcClient();
        rpc.blockWorker = 1;
        DistributedTaskScheduler scheduler = scheduler(membership, rpc,
                new SchedulerConfig(Duration.ofSeconds(2), Duration.ofSeconds(3),
                        Duration.ofMillis(20), 3, 4, 16));
        CompletableFuture<Long> result = CompletableFuture.supplyAsync(() -> {
            try {
                return scheduler.execute(JobRequest.max("depart-job", List.of(7)));
            } catch (JobExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(rpc.blockStarted.await(1, TimeUnit.SECONDS));
        membership.set(worker(2));
        assertTrue(rpc.retryStarted.await(1, TimeUnit.SECONDS));
        rpc.releaseBlocked.countDown();
        assertEquals(7, result.get(2, TimeUnit.SECONDS));
        assertTrue(rpc.calls.stream().anyMatch(c -> c.worker().getWorkerId() == 2
                && c.request().getAttempt() == 2));
    }

    @Test
    void waitsWithoutBusySpinningUntilAWorkerJoins() throws Exception {
        MutableMembership membership = new MutableMembership();
        FakeRpcClient rpc = new FakeRpcClient();
        DistributedTaskScheduler scheduler = scheduler(membership, rpc, standardConfig());
        CompletableFuture<Long> result = CompletableFuture.supplyAsync(() -> {
            try {
                return scheduler.execute(JobRequest.max("join-job", List.of(6)));
            } catch (JobExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(60);
        assertFalse(result.isDone());
        membership.set(worker(4));
        assertEquals(6, result.get(2, TimeUnit.SECONDS));
        assertTrue(membership.waitCount > 0);
    }

    @Test
    void newlyJoinedWorkerIsEligibleForSubsequentJob() throws Exception {
        MutableMembership membership = new MutableMembership(worker(1));
        FakeRpcClient rpc = new FakeRpcClient();
        DistributedTaskScheduler scheduler = scheduler(membership, rpc, standardConfig());
        assertEquals(3, scheduler.execute(JobRequest.max("first", List.of(3))));
        membership.set(worker(1), worker(2));
        assertEquals(8, scheduler.execute(JobRequest.max("second", List.of(8))));
        assertTrue(rpc.calls.stream().anyMatch(c -> c.request().getJobId().equals("second")
                && c.worker().getWorkerId() == 2));
    }

    @Test
    void taskAcceptanceIsIdempotentAndFencesOldAttempts() {
        WorkerEndpoint first = worker(1);
        WorkerEndpoint second = worker(2);
        WorkUnit unit = WorkUnit.max("job", 0, List.of(9));
        TaskAssignment task = new TaskAssignment("task", unit);
        task.assign(first, System.nanoTime() + 1_000_000);
        assertTrue(task.failAttempt(1, "timeout", 3));
        task.assign(second, System.nanoTime() + 1_000_000);

        ExecuteTaskResponse late = response("task", unit, 1, first, 9);
        ExecuteTaskResponse accepted = response("task", unit, 2, second, 9);
        assertEquals(TaskAssignment.Acceptance.STALE, task.accept(late));
        assertEquals(TaskAssignment.Acceptance.ACCEPTED, task.accept(accepted));
        assertEquals(TaskAssignment.Acceptance.STALE, task.accept(accepted));
        assertEquals(2, task.snapshot().attempt());
        assertEquals(9, task.snapshot().acceptedResult().value());
    }

    @Test
    void boundedAttemptsFailInsteadOfRetryingForever() {
        MutableMembership membership = new MutableMembership(worker(1));
        FakeRpcClient rpc = new FakeRpcClient();
        rpc.alwaysFail = true;
        DistributedTaskScheduler scheduler = scheduler(membership, rpc,
                new SchedulerConfig(Duration.ofMillis(100), Duration.ofSeconds(2),
                        Duration.ofMillis(20), 2, 2, 8));
        assertThrows(JobExecutionException.class,
                () -> scheduler.execute(JobRequest.max("failed", List.of(1))));
        assertEquals(2, rpc.calls.size());
    }

    private DistributedTaskScheduler scheduler(MutableMembership membership,
                                               FakeRpcClient rpc,
                                               SchedulerConfig config) {
        DistributedTaskScheduler scheduler = new DistributedTaskScheduler(membership, rpc, config);
        schedulers.add(scheduler);
        return scheduler;
    }

    private static SchedulerConfig standardConfig() {
        return new SchedulerConfig(Duration.ofMillis(300), Duration.ofSeconds(3),
                Duration.ofMillis(20), 3, 6, 32);
    }

    private static WorkerEndpoint worker(int id) {
        return WorkerEndpoint.newBuilder().setWorkerId(id).setHost("localhost")
                .setPort(51000 + id).setRegistrationId("registration-" + id).build();
    }

    private static ExecuteTaskResponse response(String taskId, WorkUnit unit, int attempt,
                                                WorkerEndpoint worker, long value) {
        return WorkerProtoMapper.toResponse(
                WorkerProtoMapper.toRequest(taskId, attempt, unit), worker.getWorkerId(),
                new WorkResult(unit.jobId(), unit.partitionId(), unit.operation(), value));
    }

    private static final class FakeRpcClient implements WorkerRpcClient {
        private final List<Call> calls = new CopyOnWriteArrayList<>();
        private final Map<Integer, Integer> failures = new HashMap<>();
        private final CountDownLatch blockStarted = new CountDownLatch(1);
        private final CountDownLatch releaseBlocked = new CountDownLatch(1);
        private final CountDownLatch retryStarted = new CountDownLatch(1);
        private volatile int failWorkerOnce = -1;
        private volatile int slowWorker = -1;
        private volatile long slowMillis;
        private volatile int blockWorker = -1;
        private volatile boolean alwaysFail;

        @Override
        public ExecuteTaskResponse execute(WorkerEndpoint worker, ExecuteTaskRequest request,
                                           Duration timeout) {
            calls.add(new Call(worker, request));
            if (request.getAttempt() > 1) retryStarted.countDown();
            if (alwaysFail) throw Status.UNAVAILABLE.asRuntimeException();
            synchronized (failures) {
                if (worker.getWorkerId() == failWorkerOnce
                        && failures.merge(worker.getWorkerId(), 1, Integer::sum) == 1) {
                    throw Status.UNAVAILABLE.asRuntimeException();
                }
            }
            if (worker.getWorkerId() == blockWorker) {
                blockStarted.countDown();
                try { releaseBlocked.await(2, TimeUnit.SECONDS); }
                catch (InterruptedException ignored) { /* simulate an uncooperative remote call */ }
            }
            if (worker.getWorkerId() == slowWorker) {
                try { Thread.sleep(slowMillis); }
                catch (InterruptedException ignored) { /* produce a deliberately late result */ }
            }
            WorkResult result = new WorkUnitProcessor().execute(
                    WorkerProtoMapper.toWorkUnit(request));
            return WorkerProtoMapper.toResponse(request, worker.getWorkerId(), result);
        }

        @Override public void retainWorkers(long version, Collection<WorkerEndpoint> workers) {}
        @Override public void close() { releaseBlocked.countDown(); }
    }

    private record Call(WorkerEndpoint worker, ExecuteTaskRequest request) {}

    private static final class MutableMembership implements WorkerMembership {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private List<WorkerEndpoint> workers;
        private long version = 1;
        private volatile int waitCount;

        private MutableMembership(WorkerEndpoint... workers) {
            this.workers = List.of(workers);
        }

        private void set(WorkerEndpoint... workers) {
            lock.lock();
            try {
                this.workers = List.of(workers);
                version++;
                changed.signalAll();
            } finally { lock.unlock(); }
        }

        @Override public Snapshot snapshot() {
            lock.lock();
            try { return new Snapshot(version, workers); }
            finally { lock.unlock(); }
        }

        @Override public long awaitChange(long observedVersion, Duration duration)
                throws InterruptedException {
            waitCount++;
            lock.lockInterruptibly();
            try {
                if (version == observedVersion) changed.awaitNanos(duration.toNanos());
                return version;
            } finally { lock.unlock(); }
        }

        @Override public void close() { set(); }
    }
}
