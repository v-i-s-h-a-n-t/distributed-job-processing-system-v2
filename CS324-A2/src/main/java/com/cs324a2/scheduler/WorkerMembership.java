package com.cs324a2.scheduler;

import com.cs324a2.grpc.bootstrap.WorkerEndpoint;

import java.time.Duration;
import java.util.List;

/** Thread-safe source of the current Bootstrap worker membership. */
public interface WorkerMembership extends AutoCloseable {
    Snapshot snapshot();

    /** Waits until the version changes or the duration expires. */
    long awaitChange(long observedVersion, Duration duration) throws InterruptedException;

    @Override
    void close();

    record Snapshot(long version, List<WorkerEndpoint> workers) {
        public Snapshot {
            workers = List.copyOf(workers);
        }
    }
}
