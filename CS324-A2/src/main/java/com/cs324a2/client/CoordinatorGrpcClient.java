package com.cs324a2.client;

import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.coordinator.CoordinatorJobRequest;
import com.cs324a2.grpc.coordinator.CoordinatorJobResponse;
import com.cs324a2.grpc.coordinator.CoordinatorServiceGrpc;
import com.cs324a2.grpc.coordinator.JobProgressRequest;
import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import com.cs324a2.grpc.coordinator.JobSpec;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Low-level gRPC client for coordinator nodes. Channels are cached per
 * host:port. Supports blocking job submission and both async and blocking
 * progress streaming.
 */
public final class CoordinatorGrpcClient implements AutoCloseable {
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private volatile boolean closed;

    private ManagedChannel channelFor(String host, int port) {
        String key = host + ":" + port;
        return channels.computeIfAbsent(key, ignored ->
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    public CoordinatorJobResponse submit(String host, int port, JobSpec job, long timeoutMillis) {
        if (closed) throw new IllegalStateException("coordinator gRPC client is closed");
        return CoordinatorServiceGrpc.newBlockingStub(channelFor(host, port))
                .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .submitJob(CoordinatorJobRequest.newBuilder().setJob(job).build());
    }

    /** Async server-streaming subscribe; caller gets a handle to cancel. */
    public WatchHandle subscribe(String host, int port, String jobId,
                                 StreamObserver<JobProgressUpdate> delegate) {
        if (closed) throw new IllegalStateException("coordinator gRPC client is closed");
        if (delegate == null) throw new IllegalArgumentException("delegate must not be null");
        ManagedChannel channel = channelFor(host, port);
        CoordinatorServiceGrpc.CoordinatorServiceStub asyncStub =
                CoordinatorServiceGrpc.newStub(channel);
        final java.util.concurrent.atomic.AtomicBoolean cancelled =
                new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicReference<io.grpc.stub.ClientCallStreamObserver<?>>
                callRef = new java.util.concurrent.atomic.AtomicReference<>();
        io.grpc.stub.ClientResponseObserver<JobProgressRequest, JobProgressUpdate> observer =
                new io.grpc.stub.ClientResponseObserver<>() {
                    @Override
                    public void beforeStart(
                            io.grpc.stub.ClientCallStreamObserver<JobProgressRequest> requestStream) {
                        callRef.set(requestStream);
                        if (cancelled.get()) {
                            requestStream.cancel("progress watch cancelled", null);
                        }
                    }

                    @Override public void onNext(JobProgressUpdate value) {
                        delegate.onNext(value);
                    }

                    @Override public void onError(Throwable error) {
                        delegate.onError(error);
                    }

                    @Override public void onCompleted() {
                        delegate.onCompleted();
                    }
                };
        asyncStub.subscribeJobProgress(
                JobProgressRequest.newBuilder().setJobId(jobId).build(), observer);
        return () -> {
            cancelled.set(true);
            var call = callRef.get();
            if (call != null) {
                try {
                    call.cancel("progress watch cancelled", null);
                } catch (RuntimeException ignored) {
                    // Best effort.
                }
            }
        };
    }

    /** Blocking iterator variant used for failover loops across endpoints. */
    public Iterator<JobProgressUpdate> subscribeBlocking(String host, int port, String jobId) {
        if (closed) throw new IllegalStateException("coordinator gRPC client is closed");
        return CoordinatorServiceGrpc.newBlockingStub(channelFor(host, port))
                .subscribeJobProgress(JobProgressRequest.newBuilder().setJobId(jobId).build());
    }

    /** Converts a domain job to its wire spec (mirrors CoordinatorJobMapper). */
    public static JobSpec toSpec(com.cs324a2.common.JobRequest request) {
        JobSpec.Builder builder = JobSpec.newBuilder()
                .setJobId(request.jobId());
        switch (request.operation()) {
            case MAX -> builder.setOperation(
                            com.cs324a2.grpc.coordinator.Operation.MAX)
                    .addAllNumbers(request.numbers()).setHasRange(false);
            case PRIMECOUNT -> builder.setOperation(
                            com.cs324a2.grpc.coordinator.Operation.PRIMECOUNT)
                    .addAllNumbers(request.numbers()).setHasRange(false);
            case PRIMESUM -> builder.setOperation(
                            com.cs324a2.grpc.coordinator.Operation.PRIMESUM)
                    .setStart(request.start()).setEnd(request.end()).setHasRange(true);
        }
        return builder.build();
    }

    /** Resolves a usable host:port for an endpoint (null host means unreachable). */
    public static boolean isReachable(WorkerEndpoint endpoint) {
        return endpoint != null
                && endpoint.getHost() != null && !endpoint.getHost().isBlank()
                && endpoint.getPort() > 0 && endpoint.getPort() <= 65_535;
    }

    @Override
    public void close() {
        closed = true;
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdownNow();
            } catch (RuntimeException ignored) {
                // Best effort during shutdown.
            }
        }
        channels.clear();
    }

    @FunctionalInterface
    public interface WatchHandle {
        void cancel();
    }
}
