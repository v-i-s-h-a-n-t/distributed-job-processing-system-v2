package com.cs324a2.scheduler;

import java.time.Duration;

public record SchedulerConfig(Duration taskTimeout, Duration jobTimeout,
                              Duration membershipWait, int maximumAttempts,
                              int dispatchThreads, int dispatchQueueCapacity) {
    public SchedulerConfig {
        requirePositive(taskTimeout, "task timeout");
        requirePositive(jobTimeout, "job timeout");
        requirePositive(membershipWait, "membership wait");
        if (maximumAttempts <= 0) throw new IllegalArgumentException("maximum attempts must be positive");
        if (dispatchThreads <= 0) throw new IllegalArgumentException("dispatch threads must be positive");
        if (dispatchQueueCapacity <= 0) {
            throw new IllegalArgumentException("dispatch queue capacity must be positive");
        }
    }

    public static SchedulerConfig defaults() {
        return new SchedulerConfig(Duration.ofSeconds(3), Duration.ofSeconds(30),
                Duration.ofMillis(250), 3, 8, 128);
    }

    private static void requirePositive(Duration duration, String label) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(label + " must be positive");
        }
    }
}
