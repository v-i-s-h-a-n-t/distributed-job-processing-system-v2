package com.cs324a2.compute;

import com.cs324a2.common.ComputeOperation;
import com.cs324a2.common.JobRequest;
import com.cs324a2.common.WorkUnit;
import java.util.ArrayList;
import java.math.BigInteger;
import java.util.List;

/**
 * Splits validated jobs into deterministic, balanced work units.
 */
public final class WorkPartitioner {
    public static final long PRIME_SUM_CHUNK_SIZE = 100_000L;

    public static long primeSumChunkSize(long end) {
        return end > 1_000_000_000_000L ? 256L : PRIME_SUM_CHUNK_SIZE;
    }

    private WorkPartitioner() {
        // Utility class
    }

    public static List<WorkUnit> partition(JobRequest request, int requestedPartitions) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        if (requestedPartitions <= 0) {
            throw new IllegalArgumentException("requestedPartitions must be positive");
        }

        return switch (request.operation()) {
            case MAX, PRIMECOUNT -> partitionNumbers(request, requestedPartitions);
            case PRIMESUM -> partitionRange(request, requestedPartitions);
        };
    }

    private static List<WorkUnit> partitionNumbers(
            JobRequest request, int requestedPartitions) {
        List<Integer> numbers = request.numbers();
        if (numbers.isEmpty()) {
            // Stage 2 permits an empty PRIMECOUNT request, whose final value is zero.
            return List.of();
        }

        int partitionCount = Math.min(requestedPartitions, numbers.size());
        int baseSize = numbers.size() / partitionCount;
        int remainder = numbers.size() % partitionCount;
        List<WorkUnit> partitions = new ArrayList<>(partitionCount);

        int offset = 0;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            int partitionSize = baseSize + (partitionId < remainder ? 1 : 0);
            List<Integer> values = numbers.subList(offset, offset + partitionSize);
            partitions.add(numberWorkUnit(request, partitionId, values));
            offset += partitionSize;
        }

        return List.copyOf(partitions);
    }

    private static WorkUnit numberWorkUnit(
            JobRequest request, int partitionId, List<Integer> values) {
        if (request.operation() == ComputeOperation.MAX) {
            return WorkUnit.max(request.jobId(), partitionId, values);
        }
        return WorkUnit.primeCount(request.jobId(), partitionId, values);
    }

    private static List<WorkUnit> partitionRange(
            JobRequest request, int requestedPartitions) {
        BigInteger valueCount = BigInteger.valueOf(request.end())
                .subtract(BigInteger.valueOf(request.start())).add(BigInteger.ONE);
        int partitionCount = valueCount.min(BigInteger.valueOf(requestedPartitions)).intValueExact();
        List<WorkUnit> partitions = new ArrayList<>(partitionCount);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            BigInteger[] bounds = rangeBounds(request, partitionCount, partitionId);
            long nextStart = bounds[0].longValueExact();
            long partitionEnd = bounds[1].min(bounds[0].add(
                    BigInteger.valueOf(primeSumChunkSize(request.end()) - 1))).longValueExact();
            partitions.add(WorkUnit.primeSum(
                    request.jobId(), partitionId, nextStart, partitionEnd));
        }

        return List.copyOf(partitions);
    }

    public static long partitionEnd(JobRequest request, int partitionCount, int partitionId) {
        return rangeBounds(request, partitionCount, partitionId)[1].longValueExact();
    }

    public static BigInteger[] partitionBounds(JobRequest request, int partitionCount,
                                               int partitionId) {
        return rangeBounds(request, partitionCount, partitionId);
    }

    private static BigInteger[] rangeBounds(JobRequest request, int count, int id) {
        BigInteger start = BigInteger.valueOf(request.start());
        BigInteger[] division = BigInteger.valueOf(request.end()).subtract(start)
                .add(BigInteger.ONE).divideAndRemainder(BigInteger.valueOf(count));
        BigInteger index = BigInteger.valueOf(id);
        BigInteger offset = division[0].multiply(index)
                .add(index.min(division[1]));
        BigInteger size = division[0].add(id < division[1].intValue() ? BigInteger.ONE : BigInteger.ZERO);
        BigInteger first = start.add(offset);
        return new BigInteger[]{first, first.add(size).subtract(BigInteger.ONE)};
    }
}
