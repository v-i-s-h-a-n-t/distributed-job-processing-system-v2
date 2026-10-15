package com.cs324a2.scheduler;

import com.cs324a2.common.ComputeOperation;
import com.cs324a2.common.JobRequest;
import com.cs324a2.common.WorkResult;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.compute.ResultAggregator;
import com.cs324a2.compute.WorkPartitioner;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.worker.WorkerProtoMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Completion-driven coordinator scheduler. Election/state recovery is supplied externally. */
public final class DistributedTaskScheduler implements AutoCloseable {
    private static final long COMPLETION_POLL_MILLIS = 50;

    private final WorkerMembership membership;
    private final WorkerRpcClient rpcClient;
    private final SchedulerConfig config;
    private final ExecutorService dispatchExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger workerCursor = new AtomicInteger();

    public DistributedTaskScheduler(WorkerMembership membership,
                                    WorkerRpcClient rpcClient,
                                    SchedulerConfig config) {
        if (membership == null || rpcClient == null || config == null) {
            throw new IllegalArgumentException("scheduler dependencies must not be null");
        }
        this.membership = membership;
        this.rpcClient = rpcClient;
        this.config = config;
        dispatchExecutor = new ThreadPoolExecutor(
                config.dispatchThreads(), config.dispatchThreads(), 0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.dispatchQueueCapacity()), task -> {
                    Thread thread = new Thread(task, "coordinator-task-dispatch");
                    thread.setDaemon(false);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    public long execute(JobRequest request) throws JobExecutionException {
        if (closed.get()) throw new JobExecutionException("scheduler is closed");
        if (request == null) throw new IllegalArgumentException("request must not be null");
        if (request.operation() == ComputeOperation.PRIMECOUNT && request.numbers().isEmpty()) {
            return 0L;
        }

        long jobDeadline = deadlineAfter(config.jobTimeout());
        WorkerMembership.Snapshot initial = awaitWorkers(jobDeadline);
        List<WorkUnit> units = WorkPartitioner.partition(request, initial.workers().size());
        if (request.operation() == ComputeOperation.PRIMESUM) {
            for (WorkUnit unit : units) {
                if (unit.end() < WorkPartitioner.partitionEnd(
                        request, units.size(), unit.partitionId())) {
                    throw new JobExecutionException("large PRIMESUM jobs require the checkpointed coordinator API");
                }
            }
        }
        List<TaskAssignment> tasks = new ArrayList<>(units.size());
        for (WorkUnit unit : units) {
            tasks.add(new TaskAssignment(taskId(unit), unit));
        }

        CompletionService<AttemptOutcome> completions =
                new ExecutorCompletionService<>(dispatchExecutor);
        Map<Future<AttemptOutcome>, RunningAttempt> active = new HashMap<>();

        try {
            while (true) {
                ensureJobTime(request.jobId(), jobDeadline, tasks, active);
                WorkerMembership.Snapshot snapshot = membership.snapshot();
                rpcClient.retainWorkers(snapshot.version(), snapshot.workers());
                recoverDepartedOrTimedOut(tasks, active, snapshot.workers());
                try {
                    dispatchPending(tasks, snapshot.workers(), completions, active);
                } catch (RejectedExecutionException e) {
                    cancelAll(active);
                    throw new JobExecutionException("coordinator dispatch queue is full", e);
                }

                if (tasks.stream().allMatch(t -> t.snapshot().status() == TaskStatus.COMPLETED)) {
                    List<WorkResult> results = tasks.stream()
                            .map(t -> t.snapshot().acceptedResult()).toList();
                    return ResultAggregator.aggregate(request, units, results);
                }
                TaskAssignment.Snapshot failed = tasks.stream().map(TaskAssignment::snapshot)
                        .filter(t -> t.status() == TaskStatus.FAILED).findFirst().orElse(null);
                if (failed != null) {
                    throw new JobExecutionException("task " + failed.taskId()
                            + " failed after " + failed.attempt() + " attempts: "
                            + failed.lastFailure());
                }

                Future<AttemptOutcome> completed = completions.poll(
                        COMPLETION_POLL_MILLIS, TimeUnit.MILLISECONDS);
                if (completed != null) processCompletion(completed, active);

                if (active.isEmpty() && snapshot.workers().isEmpty()) {
                    Duration wait = boundedWait(jobDeadline, config.membershipWait());
                    membership.awaitChange(snapshot.version(), wait);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cancelAll(active);
            throw new JobExecutionException("job interrupted", e);
        } catch (ArithmeticException | IllegalArgumentException e) {
            cancelAll(active);
            throw new JobExecutionException("job aggregation failed", e);
        }
    }

    private WorkerMembership.Snapshot awaitWorkers(long jobDeadline)
            throws JobExecutionException {
        while (true) {
            WorkerMembership.Snapshot snapshot = membership.snapshot();
            if (!snapshot.workers().isEmpty()) return snapshot;
            if (System.nanoTime() >= jobDeadline) {
                throw new JobExecutionException("no workers became available before job deadline");
            }
            try {
                membership.awaitChange(snapshot.version(),
                        boundedWait(jobDeadline, config.membershipWait()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new JobExecutionException("interrupted while waiting for workers", e);
            }
        }
    }

    private void dispatchPending(List<TaskAssignment> tasks,
                                 List<WorkerEndpoint> workers,
                                 CompletionService<AttemptOutcome> completions,
                                 Map<Future<AttemptOutcome>, RunningAttempt> active) {
        if (workers.isEmpty()) return;
        for (TaskAssignment task : tasks) {
            TaskAssignment.Snapshot state = task.snapshot();
            if (state.status() != TaskStatus.PENDING) continue;
            WorkerEndpoint target = chooseWorker(workers, state.target());
            long deadline = deadlineAfter(config.taskTimeout());
            TaskAssignment.Attempt attempt = task.assign(target, deadline);
            ExecuteTaskRequest request = WorkerProtoMapper.toRequest(
                    state.taskId(), attempt.number(), state.task());
            if (attempt.number() > 1) {
                log("REASSIGN", state, target, attempt.number(), null);
            } else {
                log("ASSIGN", state, target, attempt.number(), null);
            }
            Future<AttemptOutcome> future = completions.submit(() -> {
                try {
                    return AttemptOutcome.success(task, attempt.number(), target,
                            rpcClient.execute(target, request, config.taskTimeout()));
                } catch (Throwable error) {
                    return AttemptOutcome.failure(task, attempt.number(), target, error);
                }
            });
            active.put(future, new RunningAttempt(task, attempt.number(), target, deadline));
        }
    }

    private void processCompletion(Future<AttemptOutcome> completed,
                                   Map<Future<AttemptOutcome>, RunningAttempt> active) {
        RunningAttempt running = active.remove(completed);
        if (running == null) return;
        try {
            AttemptOutcome outcome = completed.get();
            if (outcome.error() != null) {
                fail(outcome.task(), outcome.attempt(), outcome.worker(), outcome.error());
                return;
            }
            TaskAssignment.Acceptance acceptance = outcome.task().accept(outcome.response());
            if (acceptance == TaskAssignment.Acceptance.ACCEPTED) {
                log("COMPLETE", outcome.task().snapshot(), outcome.worker(),
                        outcome.attempt(), "value=" + outcome.response().getValue());
            } else if (acceptance == TaskAssignment.Acceptance.INVALID) {
                fail(outcome.task(), outcome.attempt(), outcome.worker(),
                        new IllegalStateException("worker returned mismatched result identity"));
            } else {
                log("IGNORE_STALE", outcome.task().snapshot(), outcome.worker(),
                        outcome.attempt(), null);
            }
        } catch (CancellationException ignored) {
            // Departure/timeout recovery already changed the task state.
        } catch (Exception e) {
            fail(running.task(), running.attempt(), running.worker(), e);
        }
    }

    private void recoverDepartedOrTimedOut(
            List<TaskAssignment> tasks,
            Map<Future<AttemptOutcome>, RunningAttempt> active,
            List<WorkerEndpoint> workers) {
        Set<String> live = new HashSet<>();
        for (WorkerEndpoint worker : workers) live.add(registrationKey(worker));
        long now = System.nanoTime();
        new ArrayList<>(active.entrySet()).forEach(entry -> {
            RunningAttempt running = entry.getValue();
            String reason = null;
            if (!live.contains(registrationKey(running.worker()))) reason = "worker departed";
            else if (now >= running.deadlineNanos()) reason = "task timeout";
            if (reason != null && active.remove(entry.getKey()) != null) {
                entry.getKey().cancel(true);
                boolean changed = running.task().failAttempt(running.attempt(), reason,
                        config.maximumAttempts());
                if (changed) {
                    log(reason.equals("task timeout") ? "TIMEOUT" : "WORKER_LEFT",
                            running.task().snapshot(), running.worker(), running.attempt(), reason);
                }
            }
        });
    }

    private void fail(TaskAssignment task, int attempt,
                      WorkerEndpoint worker, Throwable error) {
        String description = describe(error);
        if (task.failAttempt(attempt, description, config.maximumAttempts())) {
            String event = error instanceof StatusRuntimeException status
                    && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED
                    ? "TIMEOUT" : "FAIL";
            log(event, task.snapshot(), worker, attempt, description);
        } else {
            log("IGNORE_STALE", task.snapshot(), worker, attempt, description);
        }
    }

    private WorkerEndpoint chooseWorker(List<WorkerEndpoint> workers,
                                        WorkerEndpoint previous) {
        int size = workers.size();
        for (int checked = 0; checked < size; checked++) {
            WorkerEndpoint candidate = workers.get(
                    Math.floorMod(workerCursor.getAndIncrement(), size));
            if (size == 1 || previous == null
                    || !registrationKey(candidate).equals(registrationKey(previous))) {
                return candidate;
            }
        }
        return workers.get(Math.floorMod(workerCursor.getAndIncrement(), size));
    }

    private void ensureJobTime(String jobId, long deadline,
                               List<TaskAssignment> tasks,
                               Map<Future<AttemptOutcome>, RunningAttempt> active)
            throws JobExecutionException {
        if (System.nanoTime() >= deadline) {
            cancelAll(active);
            throw new JobExecutionException("job " + jobId + " exceeded its deadline; states="
                    + tasks.stream().map(TaskAssignment::snapshot)
                    .map(s -> s.taskId() + ":" + s.status()).toList());
        }
    }

    private static void cancelAll(Map<Future<AttemptOutcome>, RunningAttempt> active) {
        active.keySet().forEach(future -> future.cancel(true));
        active.clear();
    }

    private static String taskId(WorkUnit unit) {
        return unit.jobId() + ":task:" + unit.partitionId();
    }

    private static String registrationKey(WorkerEndpoint worker) {
        return worker.getWorkerId() + ":" + worker.getRegistrationId();
    }

    private static long deadlineAfter(Duration duration) {
        long now = System.nanoTime();
        long nanos = duration.toNanos();
        return Long.MAX_VALUE - now < nanos ? Long.MAX_VALUE : now + nanos;
    }

    private static Duration boundedWait(long deadline, Duration requested) {
        long remaining = Math.max(1L, deadline - System.nanoTime());
        return Duration.ofNanos(Math.min(remaining, requested.toNanos()));
    }

    private static String describe(Throwable error) {
        if (error instanceof StatusRuntimeException status) {
            return status.getStatus().getCode() + ": " + status.getStatus().getDescription();
        }
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }

    private static void log(String event, TaskAssignment.Snapshot task,
                            WorkerEndpoint worker, int attempt, String detail) {
        System.out.printf("[Scheduler] event=%s jobId=%s taskId=%s workerId=%d attempt=%d%s%n",
                event, task.task().jobId(), task.taskId(), worker.getWorkerId(), attempt,
                detail == null ? "" : " detail=" + detail);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        dispatchExecutor.shutdownNow();
        rpcClient.close();
        membership.close();
    }

    private record RunningAttempt(TaskAssignment task, int attempt,
                                  WorkerEndpoint worker, long deadlineNanos) {}

    private record AttemptOutcome(TaskAssignment task, int attempt,
                                  WorkerEndpoint worker,
                                  ExecuteTaskResponse response, Throwable error) {
        static AttemptOutcome success(TaskAssignment task, int attempt,
                                      WorkerEndpoint worker, ExecuteTaskResponse response) {
            return new AttemptOutcome(task, attempt, worker, response, null);
        }

        static AttemptOutcome failure(TaskAssignment task, int attempt,
                                      WorkerEndpoint worker, Throwable error) {
            return new AttemptOutcome(task, attempt, worker, null, error);
        }
    }
}
