package com.cs324a2.client;

import com.cs324a2.bootstrap.BootstrapClient;
import com.cs324a2.common.JobRequest;
import com.cs324a2.common.ComputeOperation;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.coordinator.CoordinatorJobResponse;
import com.cs324a2.grpc.coordinator.JobStateProto;
import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import com.cs324a2.grpc.coordinator.JobSpec;
import com.cs324a2.grpc.coordinator.JobStatus;
import com.cs324a2.grpc.coordinator.TaskStatus;
import com.cs324a2.grpc.coordinator.WorkerProgress;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** gRPC client used by the Swing client for discovery, submit, and progress. */
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

    public List<WorkerEndpoint> listWorkers() {
        try (BootstrapClient client = new BootstrapClient(bootstrapHost, bootstrapPort)) {
            return client.listWorkers().activeWorkers();
        }
    }

    /** True if bootstrap is reachable. */
    public boolean isAvailable() {
        try {
            listWorkers();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Reads checkpoint state when a fast job outruns its progress stream. */
    public JobProgressUpdate fetchJobProgress(String jobId) {
        try (CoordinatorGrpcClient reader = new CoordinatorGrpcClient()) {
            for (WorkerEndpoint endpoint : ordered(listWorkers())) {
                try {
                    var state = reader.getState(endpoint.getHost(), endpoint.getPort(), 2_000);
                    for (JobStateProto job : state.getJobsList()) {
                        if (!job.getJob().getJobId().equals(jobId)) continue;
                        if (job.getStatus() != JobStatus.JOB_ACTIVE) {
                            return progressFromState(job);
                        }
                    }
                } catch (RuntimeException ignored) {
                    // task-only worker, no coordinator state
                }
            }
        }
        return null;
    }

    private static JobProgressUpdate progressFromState(JobStateProto job) {
        int total = job.getTasksCount();
        int completed = 0, running = 0, pending = 0, failed = 0;
        Map<Integer, int[]> perWorker = new HashMap<>();
        for (var task : job.getTasksList()) {
            int[] counts = perWorker.computeIfAbsent(task.getAssignedWorkerId(),
                    ignored -> new int[5]);
            counts[0]++;
            TaskStatus status = task.getStatus();
            if (status == TaskStatus.TASK_COMPLETED) { completed++; counts[1]++; }
            else if (status == TaskStatus.TASK_RUNNING) { running++; counts[2]++; }
            else if (status == TaskStatus.TASK_FAILED) { failed++; counts[4]++; }
            else { pending++; counts[3]++; }
        }
        JobProgressUpdate.Builder update = JobProgressUpdate.newBuilder()
                .setJobId(job.getJob().getJobId()).setTotalTasks(total)
                .setCompletedTasks(completed).setRunningTasks(running)
                .setPendingTasks(pending).setFailedTasks(failed)
                .setCompletionFraction(total == 0 ? 1.0 : completed / (double) total)
                .setProgressIndex(job.getProgressIndex()).setStatus(job.getStatus())
                .setJobState(job).setHasResult(job.getHasResult())
                .setResultValue(job.getResultValue()).setError(job.getError());
        if (!job.getExactResultValue().isEmpty()) {
            update.setExactResultValue(job.getExactResultValue());
        }
        perWorker.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(entry -> update.addWorkerBreakdown(WorkerProgress.newBuilder()
                        .setWorkerId(entry.getKey()).setTotalTasks(entry.getValue()[0])
                        .setCompletedTasks(entry.getValue()[1])
                        .setRunningTasks(entry.getValue()[2])
                        .setPendingTasks(entry.getValue()[3])
                        .setFailedTasks(entry.getValue()[4]).build()));
        return update.build();
    }

    /** Submits via gRPC, following NOT_LEADER hints. */
    public long submit(JobRequest request) throws Exception {
        return Long.parseLong(submitExact(request));
    }

    /** Exact decimal result, may exceed signed 64-bit. */
    public String submitExact(JobRequest request) throws Exception {
        while (!closed && !Thread.currentThread().isInterrupted()) {
            try {
                return submitOnce(request);
            } catch (StatusRuntimeException unavailable) {
                if (!retryable(unavailable)) throw unavailable;
                try {
                    JobProgressUpdate terminal = fetchJobProgress(request.jobId());
                    if (terminal != null) {
                        if (terminal.getStatus() == JobStatus.JOB_FAILED) {
                            throw new JobFailedException(terminal.getError());
                        }
                        return terminal.getExactResultValue().isEmpty()
                                ? Long.toString(terminal.getResultValue())
                                : terminal.getExactResultValue();
                    }
                } catch (StatusRuntimeException ignored) {
                    // still recovering
                }
                Thread.sleep(500);
            }
        }
        throw new InterruptedException("job submission stopped");
    }

    private static boolean retryable(StatusRuntimeException error) {
        return error.getStatus().getCode() == Status.Code.UNAVAILABLE
                || error.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED
                || error.getStatus().getCode() == Status.Code.CANCELLED
                // interrupted SubmitJob can surface as UNKNOWN; job ID makes retry safe
                || error.getStatus().getCode() == Status.Code.UNKNOWN;
    }

    private String submitOnce(JobRequest request) throws Exception {
        if (closed) throw new IllegalStateException("client is closed");
        List<WorkerEndpoint> endpoints = ordered(listWorkers());
        if (endpoints.isEmpty()) {
            throw new StatusRuntimeException(Status.UNAVAILABLE
                    .withDescription("no active workers registered at bootstrap"));
        }
        JobSpec spec = CoordinatorGrpcClient.toSpec(request);
        long submitTimeout = request.operation() == ComputeOperation.PRIMESUM
                ? 0 : SUBMIT_TIMEOUT_MILLIS;
        StatusRuntimeException lastFailure = null;
        // first pass: every reachable endpoint
        String hintHost = null;
        int hintPort = -1;
        for (WorkerEndpoint endpoint : endpoints) {
            if (!CoordinatorGrpcClient.isReachable(endpoint)) continue;
            try {
                CoordinatorJobResponse response = coordinators.submit(
                        endpoint.getHost(), endpoint.getPort(), spec, submitTimeout);
                if (response.getExecuted()) {
                    if (!response.getError().isEmpty()) {
                        throw new JobFailedException("job " + request.jobId()
                                + " failed: " + response.getError());
                    }
                    return response.getExactResultValue().isEmpty()
                            ? Long.toString(response.getResultValue())
                            : response.getExactResultValue();
                }
                if (!response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
                    hintHost = response.getLeaderHost();
                    hintPort = response.getLeaderPort();
                }
            } catch (JobFailedException e) {
                throw e;
            } catch (StatusRuntimeException e) {
                if (!retryable(e)) throw e;
                lastFailure = e;
            }
        }
        // second pass: leader hint
        if (hintHost != null) {
            try {
                CoordinatorJobResponse response = coordinators.submit(
                        hintHost, hintPort, spec, submitTimeout);
                if (response.getExecuted()) {
                    if (!response.getError().isEmpty()) {
                        throw new JobFailedException("job " + request.jobId()
                                + " failed: " + response.getError());
                    }
                    return response.getExactResultValue().isEmpty()
                            ? Long.toString(response.getResultValue())
                            : response.getExactResultValue();
                }
            } catch (JobFailedException e) {
                throw e;
            } catch (StatusRuntimeException e) {
                if (!retryable(e)) throw e;
                lastFailure = e;
            }
        }
        throw new StatusRuntimeException(Status.UNAVAILABLE
                .withDescription("all coordinators failed for job " + request.jobId())
                .withCause(lastFailure));
    }

    /**
     * Streams progress for a job. Roams endpoints on failover so a
     * coordinator crash doesn't kill the stream.
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
            }
        };
    }

    private void streamLoop(String jobId, ProgressListener listener,
                            CoordinatorGrpcClient streamClient, AtomicBoolean cancelled) {
        int index = 0;
        try {
            while (!cancelled.get()) {
                List<WorkerEndpoint> endpoints;
                try {
                    endpoints = ordered(listWorkers());
                } catch (RuntimeException unavailable) {
                    endpoints = List.of();
                }
                if (!endpoints.isEmpty()) {
                    WorkerEndpoint endpoint = endpoints.get(index++ % endpoints.size());
                    if (CoordinatorGrpcClient.isReachable(endpoint)) {
                        try {
                            Iterator<JobProgressUpdate> stream = streamClient.subscribeBlocking(
                                    endpoint.getHost(), endpoint.getPort(), jobId);
                            while (!cancelled.get() && stream.hasNext()) {
                                JobProgressUpdate update = stream.next();
                                listener.onUpdate(update);
                                if (update.getStatus() != JobStatus.JOB_ACTIVE) {
                                    listener.onCompleted();
                                    return;
                                }
                            }
                        } catch (StatusRuntimeException unavailable) {
                            if (cancelled.get()) return;
                        } catch (RuntimeException unavailable) {
                            if (cancelled.get()) return;
                        }
                    }
                }
                Thread.sleep(200);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            closeQuietly(streamClient);
        }
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
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            coordinators.close();
        } catch (RuntimeException ignored) {
        }
    }

    /** Progress stream callback (invoked off the EDT). */
    public interface ProgressListener {
        void onUpdate(JobProgressUpdate update);
        default void onError(Throwable error) {}
        default void onCompleted() {}
    }

    @FunctionalInterface
    public interface ProgressHandle {
        void cancel();
    }

    /** Job failed with the coordinator's error string. */
    public static final class JobFailedException extends Exception {
        JobFailedException(String message) {
            super(message);
        }
    }
}
