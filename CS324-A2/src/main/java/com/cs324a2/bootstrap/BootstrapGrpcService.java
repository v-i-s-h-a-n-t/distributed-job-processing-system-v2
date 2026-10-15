package com.cs324a2.bootstrap;

import com.cs324a2.grpc.bootstrap.BootstrapServiceGrpc;
import com.cs324a2.grpc.bootstrap.DeregisterWorkerRequest;
import com.cs324a2.grpc.bootstrap.DeregisterWorkerResponse;
import com.cs324a2.grpc.bootstrap.GetRandomWorkerRequest;
import com.cs324a2.grpc.bootstrap.GetRandomWorkerResponse;
import com.cs324a2.grpc.bootstrap.HeartbeatRequest;
import com.cs324a2.grpc.bootstrap.HeartbeatResponse;
import com.cs324a2.grpc.bootstrap.ListWorkersRequest;
import com.cs324a2.grpc.bootstrap.ListWorkersResponse;
import com.cs324a2.grpc.bootstrap.MembershipEvent;
import com.cs324a2.grpc.bootstrap.RegisterWorkerRequest;
import com.cs324a2.grpc.bootstrap.RegisterWorkerResponse;
import com.cs324a2.grpc.bootstrap.WatchMembershipRequest;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.bootstrap.WorkerRpcResultRequest;
import com.cs324a2.grpc.bootstrap.WorkerRpcResultResponse;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** gRPC implementation of worker registration and live membership. */
public final class BootstrapGrpcService
        extends BootstrapServiceGrpc.BootstrapServiceImplBase
        implements AutoCloseable {

    public static final long HEARTBEAT_INTERVAL_MILLIS = 2_000L;
    public static final long HEARTBEAT_TIMEOUT_MILLIS = 8_000L;
    private static final int MAX_QUEUED_EVENTS_PER_SUBSCRIBER = 512;

    private final Object membershipLock = new Object();
    private final Map<Integer, WorkerRegistration> workers = new HashMap<>();
    private final Map<Integer, Integer> rpcFailures = new HashMap<>();
    private final List<MembershipSubscriber> subscribers = new CopyOnWriteArrayList<>();
    private final long heartbeatTimeoutNanos;
    private final ScheduledExecutorService expiryScheduler;
    private long membershipVersion;
    private volatile boolean closed;

    public BootstrapGrpcService() {
        this(HEARTBEAT_TIMEOUT_MILLIS, HEARTBEAT_INTERVAL_MILLIS);
    }

    BootstrapGrpcService(long heartbeatTimeoutMillis, long sweepIntervalMillis) {
        if (heartbeatTimeoutMillis <= 0 || sweepIntervalMillis <= 0) {
            throw new IllegalArgumentException("heartbeat durations must be positive");
        }
        heartbeatTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(heartbeatTimeoutMillis);
        expiryScheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "bootstrap-expiry");
            thread.setDaemon(true);
            return thread;
        });
        expiryScheduler.scheduleWithFixedDelay(
                this::expireWorkers,
                sweepIntervalMillis,
                sweepIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void registerWorker(
            RegisterWorkerRequest request,
            StreamObserver<RegisterWorkerResponse> responseObserver) {
        if (request.getWorkerId() <= 0
                || request.getHost().isBlank()
                || request.getPort() <= 0
                || request.getPort() > 65_535) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("worker ID, host, or port is invalid")
                    .asRuntimeException());
            return;
        }

        RegisterWorkerResponse response;
        synchronized (membershipLock) {
            if (closed) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Bootstrap is shutting down")
                        .asRuntimeException());
                return;
            }
            expireWorkersLocked();

            int workerId = request.getWorkerId();
            if (workers.containsKey(workerId)) {
                responseObserver.onError(Status.ALREADY_EXISTS
                        .withDescription("worker " + workerId + " is already active")
                        .asRuntimeException());
                return;
            }

            WorkerEndpoint endpoint = WorkerEndpoint.newBuilder()
                    .setWorkerId(workerId)
                    .setHost(request.getHost().trim())
                    .setPort(request.getPort())
                    .setRegistrationId(UUID.randomUUID().toString())
                    .build();
            workers.put(workerId, new WorkerRegistration(endpoint, System.nanoTime()));
            rpcFailures.remove(workerId);

            long version = ++membershipVersion;
            publishLocked(MembershipEvent.newBuilder()
                    .setType(MembershipEvent.Type.WORKER_JOINED)
                    .setWorker(endpoint)
                    .setMembershipVersion(version)
                    .build());

            response = RegisterWorkerResponse.newBuilder()
                    .setRegistrationId(endpoint.getRegistrationId())
                    .addAllActiveWorkers(activeSnapshotLocked())
                    .setMembershipVersion(version)
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        System.out.printf("Bootstrap registered worker %d at %s:%d%n",
                request.getWorkerId(), request.getHost(), request.getPort());
    }

    @Override
    public void heartbeat(
            HeartbeatRequest request,
            StreamObserver<HeartbeatResponse> responseObserver) {
        boolean accepted;
        synchronized (membershipLock) {
            expireWorkersLocked();
            WorkerRegistration registration = workers.get(request.getWorkerId());
            accepted = registration != null
                    && registration.endpoint().getRegistrationId()
                            .equals(request.getRegistrationId());
            if (accepted) {
                workers.put(request.getWorkerId(),
                        registration.withHeartbeat(System.nanoTime()));
            }
        }

        responseObserver.onNext(HeartbeatResponse.newBuilder()
                .setAccepted(accepted)
                .setLeaseRemainingMillis(accepted ? HEARTBEAT_TIMEOUT_MILLIS : 0)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deregisterWorker(
            DeregisterWorkerRequest request,
            StreamObserver<DeregisterWorkerResponse> responseObserver) {
        boolean removed = false;
        synchronized (membershipLock) {
            expireWorkersLocked();
            WorkerRegistration registration = workers.get(request.getWorkerId());
            if (registration != null
                    && registration.endpoint().getRegistrationId()
                            .equals(request.getRegistrationId())
                    && workers.remove(request.getWorkerId(), registration)) {
                removed = true;
                rpcFailures.remove(request.getWorkerId());
                publishWorkerLeftLocked(registration.endpoint(), "deregistered");
            }
        }

        responseObserver.onNext(DeregisterWorkerResponse.newBuilder()
                .setRemoved(removed)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void reportWorkerRpcResult(WorkerRpcResultRequest request,
                                      StreamObserver<WorkerRpcResultResponse> observer) {
        boolean removed = false;
        synchronized (membershipLock) {
            WorkerRegistration registration = workers.get(request.getWorkerId());
            if (registration != null && registration.endpoint().getRegistrationId()
                    .equals(request.getRegistrationId())) {
                if (request.getSuccess()) {
                    rpcFailures.remove(request.getWorkerId());
                } else {
                    int failures = rpcFailures.merge(request.getWorkerId(), 1, Integer::sum);
                    if (failures >= 2 && workers.remove(request.getWorkerId(), registration)) {
                        rpcFailures.remove(request.getWorkerId());
                        publishWorkerLeftLocked(registration.endpoint(),
                                "removed after repeated RPC failures");
                        removed = true;
                    }
                }
            }
        }
        observer.onNext(WorkerRpcResultResponse.newBuilder().setRemoved(removed).build());
        observer.onCompleted();
    }

    @Override
    public void listWorkers(
            ListWorkersRequest request,
            StreamObserver<ListWorkersResponse> responseObserver) {
        ListWorkersResponse response;
        synchronized (membershipLock) {
            expireWorkersLocked();
            response = ListWorkersResponse.newBuilder()
                    .addAllActiveWorkers(activeSnapshotLocked())
                    .setMembershipVersion(membershipVersion)
                    .build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /** Suggested §5 GetActiveWorkers: alias for ListWorkers. */
    @Override
    public void getActiveWorkers(
            ListWorkersRequest request,
            StreamObserver<ListWorkersResponse> responseObserver) {
        listWorkers(request, responseObserver);
    }

    /** Suggested §5 GetRandomWorker: uniform draw over active workers. */
    @Override
    public void getRandomWorker(
            GetRandomWorkerRequest request,
            StreamObserver<GetRandomWorkerResponse> responseObserver) {
        GetRandomWorkerResponse response;
        synchronized (membershipLock) {
            expireWorkersLocked();
            List<WorkerEndpoint> candidates = activeSnapshotLocked().stream()
                    .filter(w -> w.getWorkerId() != request.getExcludeWorkerId())
                    .toList();
            if (candidates.isEmpty()) {
                response = GetRandomWorkerResponse.newBuilder()
                        .setFound(false).build();
            } else {
                WorkerEndpoint pick = candidates.get(
                        java.util.concurrent.ThreadLocalRandom.current()
                                .nextInt(candidates.size()));
                response = GetRandomWorkerResponse.newBuilder()
                        .setFound(true).setWorker(pick).build();
            }
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void watchMembership(
            WatchMembershipRequest request,
            StreamObserver<MembershipEvent> responseObserver) {
        if (!(responseObserver instanceof ServerCallStreamObserver<?> rawObserver)) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("server streaming observer is unavailable")
                    .asRuntimeException());
            return;
        }

        @SuppressWarnings("unchecked")
        ServerCallStreamObserver<MembershipEvent> serverObserver =
                (ServerCallStreamObserver<MembershipEvent>) rawObserver;
        MembershipSubscriber subscriber = new MembershipSubscriber(serverObserver);

        synchronized (membershipLock) {
            if (closed) {
                subscriber.fail(Status.UNAVAILABLE
                        .withDescription("Bootstrap is shutting down")
                        .asRuntimeException());
                return;
            }
            expireWorkersLocked();
            subscribers.add(subscriber);
            serverObserver.setOnReadyHandler(subscriber::drain);
            serverObserver.setOnCancelHandler(() -> removeSubscriber(subscriber));
            serverObserver.setOnCloseHandler(() -> removeSubscriber(subscriber));
            if (subscriber.isClosed()) return;
            // Current contract: always send a fresh snapshot, then live events.
            subscriber.send(MembershipEvent.newBuilder()
                    .setType(MembershipEvent.Type.SNAPSHOT)
                    .addAllActiveWorkers(activeSnapshotLocked())
                    .setMembershipVersion(membershipVersion)
                    .build());
        }
        subscriber.drain();
    }

    private List<WorkerEndpoint> activeSnapshotLocked() {
        return workers.values().stream()
                .map(WorkerRegistration::endpoint)
                .sorted(Comparator.comparingInt(WorkerEndpoint::getWorkerId))
                .toList();
    }

    private void expireWorkers() {
        synchronized (membershipLock) {
            if (!closed) expireWorkersLocked();
        }
    }

    private void expireWorkersLocked() {
        long now = System.nanoTime();
        workers.entrySet().removeIf(entry -> {
            WorkerRegistration registration = entry.getValue();
            if (now - registration.lastHeartbeatNanos() < heartbeatTimeoutNanos) {
                return false;
            }
            publishWorkerLeftLocked(registration.endpoint(),
                    "expired after missed heartbeats");
            rpcFailures.remove(entry.getKey());
            return true;
        });
    }

    private void publishWorkerLeftLocked(WorkerEndpoint endpoint, String reason) {
        long version = ++membershipVersion;
        publishLocked(MembershipEvent.newBuilder()
                .setType(MembershipEvent.Type.WORKER_LEFT)
                .setWorker(endpoint)
                .setMembershipVersion(version)
                .build());
        System.out.printf("Bootstrap worker %d at %s:%d %s%n",
                endpoint.getWorkerId(), endpoint.getHost(), endpoint.getPort(), reason);
    }

    private void publishLocked(MembershipEvent event) {
        for (MembershipSubscriber subscriber : subscribers) {
            subscriber.send(event);
        }
    }

    private void removeSubscriber(MembershipSubscriber subscriber) {
        synchronized (membershipLock) {
            subscribers.remove(subscriber);
            subscriber.close();
        }
    }

    @Override
    public void close() {
        synchronized (membershipLock) {
            if (closed) return;
            closed = true;
            expiryScheduler.shutdownNow();
            for (MembershipSubscriber subscriber : subscribers) {
                subscriber.complete();
            }
            subscribers.clear();
        }
    }

    private final class MembershipSubscriber {
        private final ServerCallStreamObserver<MembershipEvent> observer;
        private final ArrayDeque<MembershipEvent> queue = new ArrayDeque<>();
        private boolean closed;

        private MembershipSubscriber(ServerCallStreamObserver<MembershipEvent> observer) {
            this.observer = observer;
        }

        private synchronized void send(MembershipEvent event) {
            if (closed) return;
            if (queue.size() >= MAX_QUEUED_EVENTS_PER_SUBSCRIBER) {
                failLocked(Status.RESOURCE_EXHAUSTED
                        .withDescription("membership stream fell behind; reconnect for a fresh snapshot")
                        .asRuntimeException());
                return;
            }
            queue.addLast(event);
            drainLocked();
        }

        private synchronized void drain() {
            drainLocked();
        }

        private synchronized boolean isClosed() {
            return closed;
        }

        private void drainLocked() {
            if (closed) return;
            try {
                while (observer.isReady() && !queue.isEmpty()) {
                    observer.onNext(queue.removeFirst());
                }
            } catch (RuntimeException e) {
                closed = true;
                queue.clear();
                subscribers.remove(this);
            }
        }

        private synchronized void fail(Throwable error) {
            failLocked(error);
        }

        private void failLocked(Throwable error) {
            if (closed) return;
            closed = true;
            queue.clear();
            subscribers.remove(this);
            try {
                observer.onError(error);
            } catch (RuntimeException ignored) {
                // Stream is already unusable; removing it is sufficient.
            }
        }

        private synchronized void complete() {
            if (closed) return;
            closed = true;
            queue.clear();
            try {
                observer.onCompleted();
            } catch (RuntimeException ignored) {
                // Best effort while the Bootstrap server is shutting down.
            }
        }

        private synchronized void close() {
            closed = true;
            queue.clear();
        }
    }
}
