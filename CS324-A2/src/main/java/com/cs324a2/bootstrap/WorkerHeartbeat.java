package com.cs324a2.bootstrap;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Registers a worker and keeps its Bootstrap membership lease alive. */
public final class WorkerHeartbeat implements AutoCloseable {
    private final int workerId;
    private final String workerHost;
    private final int workerPort;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final BootstrapClient client;
    private volatile String registrationId;
    private volatile BootstrapClient.Registration initialRegistration;

    public WorkerHeartbeat(
            int workerId,
            String workerHost,
            int workerPort,
            String bootstrapHost,
            int bootstrapPort) {
        if (workerId <= 0) throw new IllegalArgumentException("worker ID must be positive");
        if (workerHost == null || workerHost.isBlank()) {
            throw new IllegalArgumentException("worker host must not be blank");
        }
        if (workerPort < 1 || workerPort > 65_535) {
            throw new IllegalArgumentException("worker port must be between 1 and 65535");
        }
        this.workerId = workerId;
        this.workerHost = workerHost;
        this.workerPort = workerPort;
        client = new BootstrapClient(bootstrapHost, bootstrapPort);
        scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "worker-" + workerId + "-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Registers immediately, starts periodic heartbeats, and returns the snapshot. */
    public BootstrapClient.Registration start() {
        if (closed.get()) throw new IllegalStateException("heartbeat manager is closed");
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("heartbeat manager already started");
        }

        try {
            initialRegistration = register();
            scheduler.scheduleWithFixedDelay(
                    this::sendHeartbeat,
                    BootstrapGrpcService.HEARTBEAT_INTERVAL_MILLIS,
                    BootstrapGrpcService.HEARTBEAT_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
            return initialRegistration;
        } catch (RuntimeException e) {
            started.set(false);
            client.close();
            scheduler.shutdownNow();
            throw e;
        }
    }

    public BootstrapClient.Registration initialRegistration() {
        if (!started.get() || initialRegistration == null) {
            throw new IllegalStateException("heartbeat manager has not started");
        }
        return initialRegistration;
    }

    private synchronized BootstrapClient.Registration register() {
        BootstrapClient.Registration registration =
                client.register(workerId, workerHost, workerPort);
        registrationId = registration.registrationId();
        System.out.printf("Worker %d registered with Bootstrap%n", workerId);
        return registration;
    }

    private void sendHeartbeat() {
        if (closed.get()) return;
        try {
            if (!client.heartbeat(workerId, registrationId) && !closed.get()) {
                register();
                System.out.printf("Worker %d restored its Bootstrap registration%n", workerId);
            }
        } catch (RuntimeException e) {
            // The next scheduled heartbeat retries after transient Bootstrap loss.
            System.err.printf("Worker %d heartbeat failed: %s%n",
                    workerId, e.getMessage());
        }
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) return;
        scheduler.shutdownNow();
        try {
            if (registrationId != null) {
                client.deregister(workerId, registrationId);
            }
        } catch (RuntimeException e) {
            System.err.printf("Worker %d could not deregister; lease will expire%n", workerId);
        } finally {
            client.close();
        }
    }
}
