package com.cs324a2.scheduler;

import com.cs324a2.bootstrap.BootstrapClient;
import com.cs324a2.grpc.bootstrap.MembershipEvent;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Maintains a live worker view from Bootstrap snapshot/watch semantics. */
public final class BootstrapWorkerMembership implements WorkerMembership {
    private static final long RECONNECT_DELAY_MILLIS = 250;

    private final BootstrapClient client;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<Integer, WorkerEndpoint> workers = new HashMap<>();
    private final ScheduledExecutorService reconnectExecutor;
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong watchGeneration = new AtomicLong();
    private volatile BootstrapClient.WatchHandle watch;
    /** Local monotonic version; independent of Bootstrap restarts. */
    private long version;
    private long sourceVersion;

    public BootstrapWorkerMembership(String host, int port) {
        client = new BootstrapClient(host, port);
        reconnectExecutor = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "bootstrap-membership-watch");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        if (closed.get()) throw new IllegalStateException("membership is closed");
        refreshAndWatch();
    }

    @Override
    public Snapshot snapshot() {
        lock.lock();
        try {
            return new Snapshot(version, workers.values().stream()
                    .sorted(Comparator.comparingInt(WorkerEndpoint::getWorkerId))
                    .toList());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long awaitChange(long observedVersion, Duration duration)
            throws InterruptedException {
        long nanos = duration.toNanos();
        lock.lockInterruptibly();
        try {
            while (!closed.get() && version == observedVersion && nanos > 0) {
                nanos = changed.awaitNanos(nanos);
            }
            return version;
        } finally {
            lock.unlock();
        }
    }

    private void refreshAndWatch() {
        if (closed.get()) return;
        try {
            BootstrapClient.WorkerSnapshot snapshot = client.listWorkers();
            replace(snapshot.activeWorkers(), snapshot.membershipVersion());
            BootstrapClient.WatchHandle previous = watch;
            long generation = watchGeneration.incrementAndGet();
            watch = client.watchMembership(snapshot.membershipVersion(), observer(generation));
            if (previous != null) previous.cancel();
            reconnectScheduled.set(false);
        } catch (RuntimeException e) {
            scheduleReconnect();
        }
    }

    private StreamObserver<MembershipEvent> observer(long generation) {
        return new StreamObserver<>() {
            @Override public void onNext(MembershipEvent event) {
                if (generation == watchGeneration.get()) apply(event);
            }
            @Override public void onError(Throwable error) {
                if (generation == watchGeneration.get()) scheduleReconnect();
            }
            @Override public void onCompleted() {
                if (generation == watchGeneration.get()) scheduleReconnect();
            }
        };
    }

    private void apply(MembershipEvent event) {
        if (event.getType() == MembershipEvent.Type.SNAPSHOT) {
            replace(event.getActiveWorkersList(), event.getMembershipVersion());
            return;
        }
        boolean gap;
        lock.lock();
        try {
            if (event.getMembershipVersion() <= sourceVersion) return;
            gap = sourceVersion != 0
                    && event.getMembershipVersion() != sourceVersion + 1;
            if (!gap) {
                WorkerEndpoint worker = event.getWorker();
                if (event.getType() == MembershipEvent.Type.WORKER_JOINED) {
                    workers.put(worker.getWorkerId(), worker);
                } else if (event.getType() == MembershipEvent.Type.WORKER_LEFT) {
                    workers.computeIfPresent(worker.getWorkerId(), (id, existing) ->
                            existing.getRegistrationId().equals(worker.getRegistrationId())
                                    ? null : existing);
                }
                sourceVersion = event.getMembershipVersion();
                version++;
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
        if (gap) scheduleReconnect();
    }

    private void replace(java.util.List<WorkerEndpoint> endpoints, long newVersion) {
        lock.lock();
        try {
            workers.clear();
            for (WorkerEndpoint endpoint : endpoints) {
                workers.put(endpoint.getWorkerId(), endpoint);
            }
            sourceVersion = newVersion;
            version++;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void scheduleReconnect() {
        if (closed.get() || !reconnectScheduled.compareAndSet(false, true)) return;
        reconnectExecutor.schedule(() -> {
            reconnectScheduled.set(false);
            refreshAndWatch();
        }, RECONNECT_DELAY_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        BootstrapClient.WatchHandle current = watch;
        if (current != null) current.cancel();
        reconnectExecutor.shutdownNow();
        client.close();
        lock.lock();
        try {
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
