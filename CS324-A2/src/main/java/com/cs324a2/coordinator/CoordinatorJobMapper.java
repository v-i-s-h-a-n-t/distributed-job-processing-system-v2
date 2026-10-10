package com.cs324a2.coordinator;

import com.cs324a2.common.ComputeOperation;
import com.cs324a2.common.JobRequest;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.grpc.coordinator.JobSpec;
import com.cs324a2.grpc.coordinator.JobStateProto;
import com.cs324a2.grpc.coordinator.JobStatus;
import com.cs324a2.grpc.coordinator.Operation;
import com.cs324a2.grpc.coordinator.TaskStateProto;
import com.cs324a2.grpc.coordinator.TaskStatus;

import java.util.ArrayList;
import java.util.List;

/** Converts between domain jobs/tasks and the coordinator wire contract. */
final class CoordinatorJobMapper {
    private CoordinatorJobMapper() {}

    static JobSpec toSpec(JobRequest request) {
        JobSpec.Builder builder = JobSpec.newBuilder()
                .setJobId(request.jobId())
                .setOperation(toProto(request.operation()));
        if (request.operation() == ComputeOperation.PRIMESUM) {
            builder.setStart(request.start()).setEnd(request.end()).setHasRange(true);
        } else {
            builder.addAllNumbers(request.numbers()).setHasRange(false);
        }
        return builder.build();
    }

    static JobRequest toJobRequest(JobSpec spec) {
        if (spec == null) throw new IllegalArgumentException("job spec must not be null");
        if (spec.getJobId().isBlank()) throw new IllegalArgumentException("jobId must not be blank");
        return switch (fromProto(spec.getOperation())) {
            case MAX -> JobRequest.max(spec.getJobId(), spec.getNumbersList());
            case PRIMECOUNT -> JobRequest.primeCount(spec.getJobId(), spec.getNumbersList());
            case PRIMESUM -> {
                if (!spec.getHasRange()) throw new IllegalArgumentException("PRIMESUM needs a range");
                yield JobRequest.primeSum(spec.getJobId(), spec.getStart(), spec.getEnd());
            }
        };
    }

    static TaskStateProto toTaskProto(ReplicatedJobStore.TaskRecord task) {
        TaskStateProto.Builder builder = TaskStateProto.newBuilder()
                .setTaskId(task.taskId())
                .setPartitionId(task.unit().partitionId())
                .setStatus(toTaskStatus(task.status()))
                .setAttempt(task.attempt())
                .setAssignedWorkerId(task.assignedWorkerId())
                .setLastFailure(task.lastFailure() == null ? "" : task.lastFailure())
                .setProgressIndex(task.progressIndex());
        if (task.resultValue() != null) {
            builder.setResultValue(task.resultValue()).setHasResult(true);
        }
        WorkUnit unit = task.unit();
        if (unit.operation() == ComputeOperation.PRIMESUM) {
            builder.setStart(unit.start()).setEnd(unit.end()).setHasRange(true);
        } else {
            builder.addAllNumbers(unit.numbers()).setHasRange(false);
        }
        return builder.build();
    }

    static JobStateProto toJobProto(ReplicatedJobStore.JobRecord job) {
        JobStateProto.Builder builder = JobStateProto.newBuilder()
                .setJob(toSpec(job.request()))
                .setStatus(job.status() == ReplicatedJobStore.JobStatus.ACTIVE
                        ? JobStatus.JOB_ACTIVE
                        : job.status() == ReplicatedJobStore.JobStatus.COMPLETED
                                ? JobStatus.JOB_COMPLETED : JobStatus.JOB_FAILED)
                .setProgressIndex(job.progressIndex())
                .setTerm(job.term());
        for (ReplicatedJobStore.TaskRecord task : job.tasks()) {
            builder.addTasks(toTaskProto(task));
        }
        if (job.resultValue() != null) {
            builder.setResultValue(job.resultValue()).setHasResult(true);
        }
        if (job.error() != null) builder.setError(job.error());
        return builder.build();
    }

    static ReplicatedJobStore.JobRecord toJobRecord(JobStateProto proto) {
        JobRequest request = toJobRequest(proto.getJob());
        List<ReplicatedJobStore.TaskRecord> tasks = new ArrayList<>();
        for (TaskStateProto task : proto.getTasksList()) {
            WorkUnit unit = toWorkUnit(request, task);
            if (unit == null) continue;
            tasks.add(new ReplicatedJobStore.TaskRecord(
                    task.getTaskId(), unit, fromTaskStatus(task.getStatus()),
                    task.getAttempt(), task.getAssignedWorkerId(),
                    task.getHasResult() ? task.getResultValue() : null,
                    task.getLastFailure().isEmpty() ? null : task.getLastFailure(),
                    task.getProgressIndex()));
        }
        ReplicatedJobStore.JobStatus status = switch (proto.getStatus()) {
            case JOB_COMPLETED -> ReplicatedJobStore.JobStatus.COMPLETED;
            case JOB_FAILED -> ReplicatedJobStore.JobStatus.FAILED;
            default -> ReplicatedJobStore.JobStatus.ACTIVE;
        };
        return new ReplicatedJobStore.JobRecord(request, tasks, status,
                proto.getHasResult() ? proto.getResultValue() : null,
                proto.getError().isEmpty() ? null : proto.getError(),
                proto.getProgressIndex(), proto.getTerm());
    }

    private static WorkUnit toWorkUnit(JobRequest request, TaskStateProto task) {
        return switch (request.operation()) {
            case PRIMESUM -> WorkUnit.primeSum(request.jobId(), task.getPartitionId(),
                    task.getHasRange() ? task.getStart() : request.start(),
                    task.getHasRange() ? task.getEnd() : request.end());
            case MAX -> {
                if (task.getNumbersList().isEmpty()) yield null;
                yield WorkUnit.max(request.jobId(), task.getPartitionId(),
                        List.copyOf(task.getNumbersList()));
            }
            case PRIMECOUNT -> WorkUnit.primeCount(request.jobId(), task.getPartitionId(),
                    List.copyOf(task.getNumbersList()));
        };
    }

    private static Operation toProto(ComputeOperation operation) {
        return switch (operation) {
            case MAX -> Operation.MAX;
            case PRIMESUM -> Operation.PRIMESUM;
            case PRIMECOUNT -> Operation.PRIMECOUNT;
        };
    }

    private static ComputeOperation fromProto(Operation operation) {
        return switch (operation) {
            case MAX -> ComputeOperation.MAX;
            case PRIMESUM -> ComputeOperation.PRIMESUM;
            case PRIMECOUNT -> ComputeOperation.PRIMECOUNT;
            default -> throw new IllegalArgumentException("operation must be specified");
        };
    }

    private static TaskStatus toTaskStatus(ReplicatedJobStore.TaskState status) {
        return switch (status) {
            case PENDING -> TaskStatus.TASK_PENDING;
            case RUNNING -> TaskStatus.TASK_RUNNING;
            case COMPLETED -> TaskStatus.TASK_COMPLETED;
            case FAILED -> TaskStatus.TASK_FAILED;
        };
    }

    private static ReplicatedJobStore.TaskState fromTaskStatus(TaskStatus status) {
        return switch (status) {
            case TASK_RUNNING -> ReplicatedJobStore.TaskState.RUNNING;
            case TASK_COMPLETED -> ReplicatedJobStore.TaskState.COMPLETED;
            case TASK_FAILED -> ReplicatedJobStore.TaskState.FAILED;
            default -> ReplicatedJobStore.TaskState.PENDING;
        };
    }
}
