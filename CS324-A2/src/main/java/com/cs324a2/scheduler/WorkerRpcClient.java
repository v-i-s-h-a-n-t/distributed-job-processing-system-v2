package com.cs324a2.scheduler;

import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;

import java.time.Duration;
import java.util.Collection;

public interface WorkerRpcClient extends AutoCloseable {
    ExecuteTaskResponse execute(WorkerEndpoint worker, ExecuteTaskRequest request,
                                Duration timeout);

    void retainWorkers(long membershipVersion, Collection<WorkerEndpoint> activeWorkers);

    @Override
    void close();
}
