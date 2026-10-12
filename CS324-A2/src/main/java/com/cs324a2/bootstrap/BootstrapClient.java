package com.cs324a2.bootstrap;

import com.cs324a2.grpc.bootstrap.BootstrapServiceGrpc;
import com.cs324a2.grpc.bootstrap.DeregisterWorkerRequest;
import com.cs324a2.grpc.bootstrap.GetRandomWorkerRequest;
import com.cs324a2.grpc.bootstrap.HeartbeatRequest;
import com.cs324a2.grpc.bootstrap.ListWorkersRequest;
import com.cs324a2.grpc.bootstrap.MembershipEvent;
import com.cs324a2.grpc.bootstrap.RegisterWorkerRequest;
import com.cs324a2.grpc.bootstrap.WatchMembershipRequest;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Client wrapper used by workers and other cluster components. */
public final class BootstrapClient implements AutoCloseable {
    private static final long DEFAULT_DEADLINE_SECONDS = 2;

    private final ManagedChannel channel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub blockingStub;
    private final BootstrapServiceGrpc.BootstrapServiceStub asyncStub;

    public BootstrapClient(String host, int port) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Bootstrap host must not be blank");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("Bootstrap port must be between 1 and 65535");
        }
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        blockingStub = BootstrapServiceGrpc.newBlockingStub(channel);
        asyncStub = BootstrapServiceGrpc.newStub(channel);
    }

    /** Constructor for tests or callers that manage their own channel. */
    public BootstrapClient(ManagedChannel channel) {
        if (channel == null) {
            throw new IllegalArgumentException("channel must not be null");
        }
        this.channel = channel;
        blockingStub = BootstrapServiceGrpc.newBlockingStub(channel);
        asyncStub = BootstrapServiceGrpc.newStub(channel);
    }

    public Registration register(int workerId, String host, int port) {
        var response = blockingStub.withDeadlineAfter(
                        DEFAULT_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .registerWorker(RegisterWorkerRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setHost(host)
                        .setPort(port)
                        .build());
        return new Registration(response.getRegistrationId(),
                response.getActiveWorkersList(), response.getMembershipVersion());
    }

    public boolean heartbeat(int workerId, String registrationId) {
        return blockingStub.withDeadlineAfter(1, TimeUnit.SECONDS)
                .heartbeat(HeartbeatRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setRegistrationId(registrationId)
                        .build())
                .getAccepted();
    }

    public boolean deregister(int workerId, String registrationId) {
        return blockingStub.withDeadlineAfter(
                        DEFAULT_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .deregisterWorker(DeregisterWorkerRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setRegistrationId(registrationId)
                        .build())
                .getRemoved();
    }

    public WorkerSnapshot listWorkers() {
        var response = blockingStub.withDeadlineAfter(
                        DEFAULT_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .listWorkers(ListWorkersRequest.getDefaultInstance());
        return new WorkerSnapshot(response.getActiveWorkersList(),
                response.getMembershipVersion());
    }

    /** Suggested §5 GetActiveWorkers: alias for ListWorkers. */
    public WorkerSnapshot getActiveWorkers() {
        var response = blockingStub.withDeadlineAfter(
                        DEFAULT_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .getActiveWorkers(ListWorkersRequest.getDefaultInstance());
        return new WorkerSnapshot(response.getActiveWorkersList(),
                response.getMembershipVersion());
    }

    /** Suggested §5 GetRandomWorker: uniform draw, excluding the caller. */
    public java.util.Optional<WorkerEndpoint> getRandomWorker(int excludeWorkerId) {
        var response = blockingStub.withDeadlineAfter(
                        DEFAULT_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .getRandomWorker(GetRandomWorkerRequest.newBuilder()
                        .setExcludeWorkerId(excludeWorkerId).build());
        return response.getFound()
                ? java.util.Optional.of(response.getWorker())
                : java.util.Optional.empty();
    }

    /** Starts a watch; the server sends a snapshot followed by live changes. */
    public WatchHandle watchMembership(
            long afterVersion,
            StreamObserver<MembershipEvent> delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("membership observer must not be null");
        }

        AtomicReference<ClientCallStreamObserver<WatchMembershipRequest>> callRef =
                new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean();
        ClientResponseObserver<WatchMembershipRequest, MembershipEvent> observer =
                new ClientResponseObserver<>() {
                    @Override
                    public void beforeStart(
                            ClientCallStreamObserver<WatchMembershipRequest> requestStream) {
                        callRef.set(requestStream);
                        if (cancelled.get()) {
                            requestStream.cancel("membership watch cancelled", null);
                        }
                    }

                    @Override
                    public void onNext(MembershipEvent event) {
                        delegate.onNext(event);
                    }

                    @Override
                    public void onError(Throwable error) {
                        delegate.onError(error);
                    }

                    @Override
                    public void onCompleted() {
                        delegate.onCompleted();
                    }
                };

        asyncStub.watchMembership(
                WatchMembershipRequest.newBuilder()
                        .setAfterVersion(Math.max(0, afterVersion))
                        .build(), observer);

        return () -> {
            cancelled.set(true);
            ClientCallStreamObserver<WatchMembershipRequest> call = callRef.get();
            if (call != null) call.cancel("membership watch cancelled", null);
        };
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public record Registration(
            String registrationId,
            List<WorkerEndpoint> activeWorkers,
            long membershipVersion) {}

    public record WorkerSnapshot(
            List<WorkerEndpoint> activeWorkers,
            long membershipVersion) {}

    @FunctionalInterface
    public interface WatchHandle {
        void cancel();
    }
}
