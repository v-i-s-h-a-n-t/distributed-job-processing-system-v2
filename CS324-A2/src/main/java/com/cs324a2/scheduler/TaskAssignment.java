package com.cs324a2.scheduler;

import com.cs324a2.common.WorkResult;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.worker.WorkerProtoMapper;

/** Thread-safe lifecycle and exactly-once acceptance state for one logical task. */
public final class TaskAssignment {
    public enum Acceptance { ACCEPTED, STALE, INVALID }

    private final String taskId;
    private final WorkUnit task;
    private TaskStatus status = TaskStatus.PENDING;
    private WorkerEndpoint target;
    private WorkerEndpoint previousTarget;
    private int attempt;
    private long deadlineNanos;
    private WorkResult acceptedResult;
    private String lastFailure;

    public TaskAssignment(String taskId, WorkUnit task) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (task == null) throw new IllegalArgumentException("task must not be null");
        this.taskId = taskId;
        this.task = task;
    }

    public synchronized Attempt assign(WorkerEndpoint worker, long deadlineNanos) {
        if (status != TaskStatus.PENDING) {
            throw new IllegalStateException("only a pending task can be assigned");
        }
        previousTarget = target;
        target = worker;
        attempt++;
        this.deadlineNanos = deadlineNanos;
        status = TaskStatus.RUNNING;
        return new Attempt(attempt, worker, deadlineNanos);
    }

    public synchronized Acceptance accept(ExecuteTaskResponse response) {
        if (status == TaskStatus.COMPLETED || response.getAttempt() != attempt) {
            return Acceptance.STALE;
        }
        if (status != TaskStatus.RUNNING || target == null) return Acceptance.STALE;
        if (!taskId.equals(response.getTaskId())
                || !task.jobId().equals(response.getJobId())
                || task.partitionId() != response.getPartitionId()
                || target.getWorkerId() != response.getWorkerId()) {
            return Acceptance.INVALID;
        }
        try {
            WorkResult result = WorkerProtoMapper.toWorkResult(response);
            if (result.operation() != task.operation()) return Acceptance.INVALID;
            acceptedResult = result;
            status = TaskStatus.COMPLETED;
            return Acceptance.ACCEPTED;
        } catch (IllegalArgumentException e) {
            return Acceptance.INVALID;
        }
    }

    public synchronized boolean failAttempt(int failedAttempt, String reason,
                                            int maximumAttempts) {
        if (status != TaskStatus.RUNNING || failedAttempt != attempt) return false;
        lastFailure = reason;
        status = attempt >= maximumAttempts ? TaskStatus.FAILED : TaskStatus.PENDING;
        return true;
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(taskId, task, status, target, previousTarget, attempt,
                deadlineNanos, acceptedResult, lastFailure);
    }

    public record Attempt(int number, WorkerEndpoint target, long deadlineNanos) {}

    public record Snapshot(String taskId, WorkUnit task, TaskStatus status,
                           WorkerEndpoint target, WorkerEndpoint previousTarget,
                           int attempt, long deadlineNanos,
                           WorkResult acceptedResult, String lastFailure) {}
}
