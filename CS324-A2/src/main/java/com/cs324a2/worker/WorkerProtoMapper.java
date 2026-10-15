package com.cs324a2.worker;

import com.cs324a2.common.ComputeOperation;
import com.cs324a2.common.WorkResult;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.grpc.worker.NumberPayload;
import com.cs324a2.grpc.worker.Operation;
import com.cs324a2.grpc.worker.RangePayload;

/** Converts between the wire contract and the validated domain contract. */
public final class WorkerProtoMapper {
    private WorkerProtoMapper() {}

    public static WorkUnit toWorkUnit(ExecuteTaskRequest request) {
        validateIdentity(request);
        return switch (request.getOperation()) {
            case MAX -> {
                requirePayload(request, ExecuteTaskRequest.PayloadCase.NUMBER_PAYLOAD);
                yield WorkUnit.max(request.getJobId(), request.getPartitionId(),
                        request.getNumberPayload().getNumbersList());
            }
            case PRIMECOUNT -> {
                requirePayload(request, ExecuteTaskRequest.PayloadCase.NUMBER_PAYLOAD);
                yield WorkUnit.primeCount(request.getJobId(), request.getPartitionId(),
                        request.getNumberPayload().getNumbersList());
            }
            case PRIMESUM -> {
                requirePayload(request, ExecuteTaskRequest.PayloadCase.RANGE_PAYLOAD);
                yield WorkUnit.primeSum(request.getJobId(), request.getPartitionId(),
                        request.getRangePayload().getStart(), request.getRangePayload().getEnd());
            }
            case OPERATION_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("operation must be specified");
        };
    }

    public static ExecuteTaskRequest toRequest(
            String taskId, int attempt, WorkUnit unit) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (attempt <= 0) throw new IllegalArgumentException("attempt must be positive");
        ExecuteTaskRequest.Builder builder = ExecuteTaskRequest.newBuilder()
                .setTaskId(taskId)
                .setJobId(unit.jobId())
                .setPartitionId(unit.partitionId())
                .setAttempt(attempt)
                .setOperation(toProto(unit.operation()));
        if (unit.operation() == ComputeOperation.PRIMESUM) {
            builder.setRangePayload(RangePayload.newBuilder()
                    .setStart(unit.start()).setEnd(unit.end()));
        } else {
            builder.setNumberPayload(NumberPayload.newBuilder()
                    .addAllNumbers(unit.numbers()));
        }
        return builder.build();
    }

    public static ExecuteTaskResponse toResponse(
            ExecuteTaskRequest request, int workerId, WorkResult result) {
        return ExecuteTaskResponse.newBuilder()
                .setTaskId(request.getTaskId())
                .setJobId(result.jobId())
                .setPartitionId(result.partitionId())
                .setAttempt(request.getAttempt())
                .setWorkerId(workerId)
                .setOperation(toProto(result.operation()))
                .setValue(result.value())
                .setExactValue(result.exactValue())
                .build();
    }

    public static WorkResult toWorkResult(ExecuteTaskResponse response) {
        return new WorkResult(response.getJobId(), response.getPartitionId(),
                fromProto(response.getOperation()), response.getValue(), response.getExactValue());
    }

    private static void validateIdentity(ExecuteTaskRequest request) {
        if (request == null) throw new IllegalArgumentException("request must not be null");
        if (request.getTaskId().isBlank()) throw new IllegalArgumentException("taskId must not be blank");
        if (request.getJobId().isBlank()) throw new IllegalArgumentException("jobId must not be blank");
        if (request.getPartitionId() < 0) throw new IllegalArgumentException("partitionId must not be negative");
        if (request.getAttempt() <= 0) throw new IllegalArgumentException("attempt must be positive");
    }

    private static void requirePayload(
            ExecuteTaskRequest request, ExecuteTaskRequest.PayloadCase expected) {
        if (request.getPayloadCase() != expected) {
            throw new IllegalArgumentException(request.getOperation()
                    + " requires " + expected.name().toLowerCase());
        }
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
            case OPERATION_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("operation must be specified");
        };
    }
}
