package com.cs324a2.worker;

import com.cs324a2.bootstrap.WorkerHeartbeat;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Starts a reachable worker service and publishes it through Bootstrap. */
public final class WorkerServer implements AutoCloseable {
    private static final long SERVER_SHUTDOWN_SECONDS = 5;

    private final int workerId;
    private final String advertisedHost;
    private final int workerPort;
    private final String bootstrapHost;
    private final int bootstrapPort;
    private final WorkerGrpcService service;
    private final AtomicBoolean closed = new AtomicBoolean();
    private Server server;
    private WorkerHeartbeat heartbeat;

    public WorkerServer(int workerId, String advertisedHost, int workerPort,
                        String bootstrapHost, int bootstrapPort) {
        validatePort(workerPort, "worker port");
        validatePort(bootstrapPort, "Bootstrap port");
        if (advertisedHost == null || advertisedHost.isBlank()) {
            throw new IllegalArgumentException("advertised host must not be blank");
        }
        if (bootstrapHost == null || bootstrapHost.isBlank()) {
            throw new IllegalArgumentException("Bootstrap host must not be blank");
        }
        this.workerId = workerId;
        this.advertisedHost = advertisedHost.trim();
        this.workerPort = workerPort;
        this.bootstrapHost = bootstrapHost.trim();
        this.bootstrapPort = bootstrapPort;
        this.service = new WorkerGrpcService(workerId);
    }

    public void start() throws IOException {
        if (server != null) throw new IllegalStateException("worker server already started");
        server = ServerBuilder.forPort(workerPort).addService(service).build().start();
        try {
            heartbeat = new WorkerHeartbeat(workerId, advertisedHost, workerPort,
                    bootstrapHost, bootstrapPort);
            heartbeat.start();
            service.startAccepting();
            System.out.printf("Worker %d listening at %s:%d%n",
                    workerId, advertisedHost, workerPort);
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    public void awaitTermination() throws InterruptedException {
        if (server == null) throw new IllegalStateException("worker server has not started");
        server.awaitTermination();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        service.stopAccepting();
        if (heartbeat != null) heartbeat.close();
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(SERVER_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                server.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        service.close();
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Usage: WorkerServer <workerId> <advertisedHost> "
                    + "<workerPort> <bootstrapHost> <bootstrapPort>");
            System.exit(1);
        }
        try {
            WorkerServer worker = new WorkerServer(Integer.parseInt(args[0]), args[1],
                    Integer.parseInt(args[2]), args[3], Integer.parseInt(args[4]));
            Runtime.getRuntime().addShutdownHook(new Thread(worker::close,
                    "worker-grpc-shutdown"));
            worker.start();
            worker.awaitTermination();
        } catch (Exception e) {
            System.err.println("Worker startup failed: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void validatePort(int port, String label) {
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException(label + " must be between 1 and 65535");
        }
    }
}
