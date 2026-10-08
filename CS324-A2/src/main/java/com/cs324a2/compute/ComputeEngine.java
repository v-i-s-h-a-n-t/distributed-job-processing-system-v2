package com.cs324a2.compute;

import java.util.List;

/**
 * Pure implementations of the computations supported by the job system.
 */
public final class ComputeEngine {

    private static final int SIEVE_SEGMENT_SIZE = 1 << 20;
    private static final long MAX_SIEVE_BASE = 50_000_000L;

    private ComputeEngine() {
        // Utility class
    }

    /**
     * Returns the largest value in an unsorted list.
     *
     * @throws IllegalArgumentException if the list is null, empty, or contains null
     */
    public static int max(List<Integer> numbers) {
        validateNumbers(numbers, false);

        int maximum = numbers.get(0);
        for (int number : numbers) {
            if (number > maximum) {
                maximum = number;
            }
        }
        return maximum;
    }

    /**
     * Returns the sum of all prime numbers in the inclusive range.
     *
     * @throws IllegalArgumentException if start is greater than end
     */
    public static long primeSum(long start, long end) {
        if (start > end) {
            throw new IllegalArgumentException("start must not be greater than end");
        }
        if (end < 2) return 0L;
        long baseLimit = (long) Math.sqrt(end);
        while ((baseLimit + 1) <= end / (baseLimit + 1)) baseLimit++;
        while (baseLimit > end / baseLimit) baseLimit--;
        if (baseLimit > MAX_SIEVE_BASE) {
            throw new IllegalArgumentException("PRIMESUM endpoint is too large for the sieve (maximum is "
                    + (MAX_SIEVE_BASE * MAX_SIEVE_BASE) + ")");
        }
        int[] basePrimes = primesUpTo((int) baseLimit);
        long sum = 0;
        long segmentStart = Math.max(2L, start);
        while (segmentStart <= end) {
            long remaining = end - segmentStart;
            int segmentLength = (int) Math.min((long) SIEVE_SEGMENT_SIZE, remaining + 1L);
            long segmentEnd = segmentStart + segmentLength - 1L;
            boolean[] composite = new boolean[segmentLength];
            for (int prime : basePrimes) {
                long p = prime;
                if (p > segmentEnd / p) break;
                long first = Math.max(p * p, ((segmentStart + p - 1L) / p) * p);
                for (long multiple = first; multiple <= segmentEnd; ) {
                    composite[(int) (multiple - segmentStart)] = true;
                    if (segmentEnd - multiple < p) break;
                    multiple += p;
                }
            }
            for (int offset = 0; offset < segmentLength; offset++) {
                if (!composite[offset]) sum = Math.addExact(sum, segmentStart + offset);
            }
            if (segmentEnd == end) break;
            segmentStart = segmentEnd + 1L;
        }
        return sum;
    }

    private static int[] primesUpTo(int limit) {
        if (limit < 2) return new int[0];
        boolean[] composite = new boolean[limit + 1];
        for (int candidate = 2; candidate <= limit / candidate; candidate++) {
            if (!composite[candidate]) {
                for (int multiple = candidate * candidate; multiple <= limit; multiple += candidate) {
                    composite[multiple] = true;
                }
            }
        }
        int count = 0;
        for (int candidate = 2; candidate <= limit; candidate++) if (!composite[candidate]) count++;
        int[] primes = new int[count];
        for (int candidate = 2, index = 0; candidate <= limit; candidate++) {
            if (!composite[candidate]) primes[index++] = candidate;
        }
        return primes;
    }

    /**
     * Counts prime values in an unsorted list. Duplicate primes are counted
     * separately because each list entry is an input value.
     *
     * @throws IllegalArgumentException if the list is null or contains null
     */
    public static int primeCount(List<Integer> numbers) {
        validateNumbers(numbers, true);

        int count = 0;
        for (int number : numbers) {
            if (isPrime(number)) {
                count++;
            }
        }
        return count;
    }

    private static void validateNumbers(List<Integer> numbers, boolean allowEmpty) {
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
    }

    private static boolean isPrime(int number) {
        if (number < 2) {
            return false;
        }
        if (number == 2) {
            return true;
        }
        if (number % 2 == 0) {
            return false;
        }

        for (int divisor = 3; divisor <= number / divisor; divisor += 2) {
            if (number % divisor == 0) {
                return false;
            }
        }
        return true;
    }
}
