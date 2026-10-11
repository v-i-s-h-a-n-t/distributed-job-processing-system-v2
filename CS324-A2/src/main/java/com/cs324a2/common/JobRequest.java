package com.cs324a2.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Immutable top-level computation request submitted under a unique job ID.
 */
public record JobRequest(
        String jobId,
        ComputeOperation operation,
        List<Integer> numbers,
        Long start,
        Long end) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Maximum number of values ({@code end - start + 1}) accepted in a PRIMESUM
     * range. The segmented sieve can address far larger endpoints, but spans
     * beyond this would blow past interactive job timeouts on the demo cluster.
     * Shared by {@link JobRequest}, the client's CSV parser, and the GUI so all
     * range entry points reject the same oversized inputs.
     */
    public static final long MAX_RANGE_SPAN = 100_000_000L;

    public JobRequest {
        validateJobId(jobId);
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }

        switch (operation) {
            case MAX -> {
                numbers = immutableNumbers(numbers, false);
                requireNoRange(start, end);
            }
            case PRIMECOUNT -> {
                numbers = immutableNumbers(numbers, true);
                requireNoRange(start, end);
            }
            case PRIMESUM -> {
                if (numbers != null) {
                    throw new IllegalArgumentException("PRIMESUM must not contain a numbers list");
                }
                validateRange(start, end);
            }
        }
    }

    public static JobRequest max(String jobId, List<Integer> numbers) {
        return new JobRequest(jobId, ComputeOperation.MAX, numbers, null, null);
    }

    public static JobRequest primeSum(String jobId, long start, long end) {
        return new JobRequest(jobId, ComputeOperation.PRIMESUM, null, start, end);
    }

    public static JobRequest primeCount(String jobId, List<Integer> numbers) {
        return new JobRequest(jobId, ComputeOperation.PRIMECOUNT, numbers, null, null);
    }

    private static void validateJobId(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId must not be blank");
        }
    }

    private static List<Integer> immutableNumbers(List<Integer> numbers, boolean allowEmpty) {
        if (numbers == null) {
            throw new IllegalArgumentException("numbers must not be null");
        }
        if (!allowEmpty && numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be empty");
        }
        for (Integer number : numbers) {
            if (number == null) {
                throw new IllegalArgumentException("numbers must not contain null values");
            }
        }
        return List.copyOf(numbers);
    }

    private static void requireNoRange(Long start, Long end) {
        if (start != null || end != null) {
            throw new IllegalArgumentException("list operations must not contain range bounds");
        }
    }

    private static void validateRange(Long start, Long end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("PRIMESUM requires both range bounds");
        }
        checkRangeSpan(start, end);
    }

    /**
     * Shared PRIMESUM bounds check: ordering, 64-bit span overflow, and the
     * {@link #MAX_RANGE_SPAN} cap. Throws {@link IllegalArgumentException} with
     * a message callers can surface directly or wrap (e.g. as an
     * {@code IOException} for CSV input).
     */
    public static void checkRangeSpan(long start, long end) {
        if (start > end) {
            throw new IllegalArgumentException("start must not be greater than end");
        }
        final long span;
        try {
            span = Math.addExact(Math.subtractExact(end, start), 1L);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("PRIMESUM range is too wide", e);
        }
        if (span > MAX_RANGE_SPAN) {
            throw new IllegalArgumentException("PRIMESUM range span " + span
                    + " exceeds the maximum of " + MAX_RANGE_SPAN);
        }
    }
}
