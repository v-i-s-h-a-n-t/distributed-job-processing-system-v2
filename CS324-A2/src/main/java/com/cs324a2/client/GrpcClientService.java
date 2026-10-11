package com.cs324a2.client;

import com.cs324a2.bootstrap.BootstrapClient;
import com.cs324a2.common.JobRequest;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.coordinator.CoordinatorJobResponse;
import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import com.cs324a2.grpc.coordinator.JobSpec;
import com.cs324a2.grpc.coordinator.JobStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-level gRPC facade used by the Swing client.
 *
 * <p>Discovery goes through the gRPC Bootstrap ({@code ListWorkers}); job
 * submission goes to coordinator nodes ({@code SubmitJob}) following leader
 * hints; progress arrives via server-streaming
 * ({@code SubscribeJobProgress}) with failover across endpoints so a
 * coordinator crash mid-job does not kill the client's progress view.
 */
public final class GrpcClientService implements AutoCloseable {
    private static final long SUBMIT_TIMEOUT_MILLIS = 120_000;

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final CoordinatorGrpcClient coordinators = new CoordinatorGrpcClient();
    private volatile boolean closed;

    public GrpcClientService(String bootstrapHost, int bootstrapPort) {
        if (bootstrapHost == null || bootstrapHost.isBlank()) {
            throw new IllegalArgumentException("bootstrap host must not be blank");
        }
        if (bootstrapPort < 1 || bootstrapPort > 65_535) {
            throw new IllegalArgumentException("bootstrap port must be 1-65535");
        }
        this.bootstrapHost = bootstrapHost.trim();
        this.bootstrapPort = bootstrapPort;
    }

    /** Lists live coordinator/worker endpoints via gRPC Bootstrap. */
    public List<WorkerEndpoint> listWorkers() {
        try (BootstrapClient client = new BootstrapClient(bootstrapHost, bootstrapPort)) {
            // listWorkers() already applies a ~2s deadline internally.
            return client.listWorkers().activeWorkers();
        }
    }

    /** Quick probe used by the GUI to prefer gRPC and fall back to RMI. */
    public boolean isAvailable() {
        try {
            listWorkers();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Submits a job through gRPC, following NOT_LEADER hints. */
    public long submit(JobRequest request) throws Exception {
        if (closed) throw new IllegalStateException("client is closed");
        List<WorkerEndpoint> endpoints = ordered(listWorkers());
        if (endpoints.isEmpty()) {
            throw new StatusRuntimeException(Status.UNAVAILABLE
                    .withDescription("no active workers registered at bootstrap"));
        }
        JobSpec spec = CoordinatorGrpcClient.toSpec(request);
        StatusRuntimeException lastFailure = null;
        // First pass: try every reachable endpoint in order.
        String hintHost = null;
        int hintPort = -1;
        for (WorkerEndpoint endpoint : endpoints) {
            if (!CoordinatorGrpcClient.isReachable(endpoint)) continue;
            try {
                CoordinatorJobResponse response = coordinators.submit(
                        endpoint.getHost(), endpoint.getPort(), spec, SUBMIT_TIMEOUT_MILLIS);
                if (response.getExecuted()) {
                    if (!response.getError().isEmpty()) {
                        throw new JobFailedException("job " + request.jobId()
                                + " failed: " + response.getError());
                    }
                    return response.getResultValue();
                }
                if (!response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
                    hintHost = response.getLeaderHost();
                    hintPort = response.getLeaderPort();
                }
            } catch (JobFailedException e) {
                throw e;
            } catch (StatusRuntimeException e) {
                lastFailure = e;
            }
        }
        // Second pass: follow the leader hint if one was offered.
        if (hintHost != null) {
            try {
                CoordinatorJobResponse response = coordinators.submit(
                        hintHost, hintPort, spec, SUBMIT_TIMEOUT_MILLIS);
                if (response.getExecuted()) {
                    if (!response.getError().isEmpty()) {
                        throw new JobFailedException("job " + request.jobId()
                                + " failed: " + response.getError());
                    }
                    return response.getResultValue();
                }
            } catch (JobFailedException e) {
                throw e;
            } catch (StatusRuntimeException e) {
                lastFailure = e;
            }
        }
        throw new StatusRuntimeException(Status.UNAVAILABLE
                .withDescription("all coordinators failed for job " + request.jobId())
                .withCause(lastFailure));
    }

    /**
     * Streams real-time progress for {@code jobId}. The subscription survives
     * coordinator failover by roaming across endpoints: followers serve their
     * replicated snapshot, so killing the leader mid-job does not end the
     * stream. Updates stop on terminal state, error (all endpoints dead), or
     * {@link ProgressHandle#cancel()}.
     */
    public ProgressHandle subscribeProgress(String jobId, ProgressListener listener) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId must not be blank");
        }
        if (listener == null) throw new IllegalArgumentException("listener must not be null");
        CoordinatorGrpcClient streamClient = new CoordinatorGrpcClient();
        AtomicBoolean cancelled = new AtomicBoolean();
        Thread worker = new Thread(() -> streamLoop(jobId, listener, streamClient, cancelled),
                "job-progress-" + jobId);
        worker.setDaemon(true);
        worker.start();
        return () -> {
            cancelled.set(true);
            worker.interrupt();
            try {
                streamClient.close();
            } catch (RuntimeException ignored) {
                // Best effort.
            }
        };
    }

    private void streamLoop(String jobId, ProgressListener listener,
                            CoordinatorGrpcClient streamClient, AtomicBoolean cancelled) {
        List<WorkerEndpoint> endpoints;
        try {
            endpoints = ordered(listWorkers());
        } catch (RuntimeException e) {
            if (!cancelled.get()) listener.onError(e);
            closeQuietly(streamClient);
            return;
        }
        if (endpoints.isEmpty()) {
            listener.onError(Status.UNAVAILABLE
                    .withDescription("no active workers for progress stream").asRuntimeException());
            closeQuietly(streamClient);
            return;
        }
        // Roam across endpoints until a terminal update, cancellation, or
        // every endpoint fails consecutively without any update.
        int consecutiveFailures = 0;
        int index = 0;
        while (!cancelled.get() && !closed) {
            WorkerEndpoint endpoint = endpoints.get(index % endpoints.size());
            index++;
            if (!CoordinatorGrpcClient.isReachable(endpoint)) {
                consecutiveFailures++;
                if (consecutiveFailures >= endpoints.size()) break;
                continue;
            }
            try {
                Iterator<JobProgressUpdate> stream =
                        streamClient.subscribeBlocking(
                                endpoint.getHost(), endpoint.getPort(), jobId);
                consecutiveFailures = 0;
                while (!cancelled.get() && stream.hasNext()) {
                    JobProgressUpdate update = stream.next();
                    listener.onUpdate(update);
                    if (update.getStatus() != JobStatus.JOB_ACTIVE) {
                        listener.onCompleted();
                        closeQuietly(streamClient);
                        return;
                    }
                }
                // Server completed without a terminal update (should not
                // happen); treat as terminal to avoid spinning.
                if (!cancelled.get()) {
                    listener.onCompleted();
                    closeQuietly(streamClient);
                    return;
                }
            } catch (StatusRuntimeException e) {
                if (cancelled.get()
                        || e.getStatus().getCode() == Status.Code.CANCELLED) {
                    closeQuietly(streamClient);
                    return;
                }
                consecutiveFailures++;
                if (consecutiveFailures >= Math.max(1, endpoints.size())) {
                    // Refresh membership once before giving up (covers the
                    // post-failover replacement joining under a new port).
                    try {
                        List<WorkerEndpoint> fresh = ordered(listWorkers());
                        if (!fresh.isEmpty()) {
                            endpoints = fresh;
                            consecutiveFailures = 0;
                            continue;
                        }
                    } catch (RuntimeException ignored) {
                        // Fall through to the error below.
                    }
                    listener.onError(e);
                    closeQuietly(streamClient);
                    return;
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    closeQuietly(streamClient);
                    return;
                }
            } catch (RuntimeException e) {
                if (!cancelled.get()) listener.onError(e);
                closeQuietly(streamClient);
                return;
            }
        }
        closeQuietly(streamClient);
    }

    private static List<WorkerEndpoint> ordered(List<WorkerEndpoint> endpoints) {
        List<WorkerEndpoint> copy = new ArrayList<>(endpoints);
        copy.sort(Comparator.comparingInt(WorkerEndpoint::getWorkerId));
        return List.copyOf(copy);
    }

    private static void closeQuietly(CoordinatorGrpcClient client) {
        try {
            client.close();
        } catch (RuntimeException ignored) {
            // Best effort.
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            coordinators.close();
        } catch (RuntimeException ignored) {
            // Best effort.
        }
    }

    /** Listener for streamed progress updates (invoked off the EDT). */
    public interface ProgressListener {
        void onUpdate(JobProgressUpdate update);
        default void onError(Throwable error) {}
        default void onCompleted() {}
    }

    @FunctionalInterface
    public interface ProgressHandle {
        void cancel();
    }

    /** Checked failure carrying the coordinator's terminal error string. */
    public static final class JobFailedException extends Exception {
        JobFailedException(String message) {
            super(message);
        }
    }
}
