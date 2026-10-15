package com.cs324a2.common;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigInteger;

/**
 * Immutable scalar result produced for one work-unit partition.
 */
public record WorkResult(
        String jobId,
        int partitionId,
        ComputeOperation operation,
        long value, String exactValue) implements Serializable {

    public WorkResult(String jobId, int partitionId, ComputeOperation operation, long value) {
        this(jobId, partitionId, operation, value, "");
    }

    public static WorkResult exactPrimeSum(String jobId, int partitionId, BigInteger value) {
        return new WorkResult(jobId, partitionId, ComputeOperation.PRIMESUM,
                value.bitLength() <= 63 ? value.longValue() : 0L, value.toString());
    }

    public BigInteger bigValue() {
        return exactValue.isEmpty() ? BigInteger.valueOf(value) : new BigInteger(exactValue);
    }

    @Serial
    private static final long serialVersionUID = 1L;

    public WorkResult {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId must not be blank");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must not be negative");
        }
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }
        if (exactValue == null) throw new IllegalArgumentException("exactValue must not be null");
        if (!exactValue.isEmpty() && new BigInteger(exactValue).signum() < 0) {
            throw new IllegalArgumentException("exact result must not be negative");
        }
        if ((operation == ComputeOperation.PRIMESUM
                || operation == ComputeOperation.PRIMECOUNT) && value < 0
                && exactValue.isEmpty()) {
            throw new IllegalArgumentException(operation + " result must not be negative");
        }
    }
}
