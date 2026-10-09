package com.cs324a2.scheduler;

import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.grpc.worker.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** Reuses channels for live worker registration instances. */
public final class GrpcWorkerRpcClient implements WorkerRpcClient {
    private final ConcurrentHashMap<EndpointKey, ManagedChannel> channels =
            new ConcurrentHashMap<>();
    private final AtomicLong retainedMembershipVersion = new AtomicLong(-1);

    @Override
    public ExecuteTaskResponse execute(WorkerEndpoint worker,
                                       ExecuteTaskRequest request,
                                       Duration timeout) {
        EndpointKey key = EndpointKey.of(worker);
        ManagedChannel channel = channels.computeIfAbsent(key, ignored ->
                ManagedChannelBuilder.forAddress(worker.getHost(), worker.getPort())
                        .usePlaintext().build());
        return WorkerServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                .executeTask(request);
    }

    @Override
    public void retainWorkers(long membershipVersion,
                              Collection<WorkerEndpoint> activeWorkers) {
        long previous = retainedMembershipVersion.getAndAccumulate(
                membershipVersion, Math::max);
        if (membershipVersion < previous) return;
        Set<EndpointKey> active = activeWorkers.stream()
                .map(EndpointKey::of).collect(Collectors.toSet());
        channels.forEach((key, channel) -> {
            if (!active.contains(key) && channels.remove(key, channel)) {
                channel.shutdownNow();
            }
        });
    }

    @Override
    public void close() {
        channels.forEach((key, channel) -> channel.shutdownNow());
        channels.clear();
    }

    record EndpointKey(int workerId, String registrationId, String host, int port) {
        static EndpointKey of(WorkerEndpoint endpoint) {
            return new EndpointKey(endpoint.getWorkerId(), endpoint.getRegistrationId(),
                    endpoint.getHost(), endpoint.getPort());
        }
    }
}
