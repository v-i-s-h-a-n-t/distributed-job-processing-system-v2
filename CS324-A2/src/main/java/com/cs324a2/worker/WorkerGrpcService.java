package com.cs324a2.worker;

import com.cs324a2.common.WorkResult;
import com.cs324a2.common.WorkUnit;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.grpc.worker.WorkerServiceGrpc;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** gRPC boundary for bounded, concurrent worker computation. */
public final class WorkerGrpcService extends WorkerServiceGrpc.WorkerServiceImplBase
        implements AutoCloseable {
    public static final int DEFAULT_COMPUTE_THREADS = 4;
    public static final int DEFAULT_QUEUE_CAPACITY = 64;
    private static final long SHUTDOWN_SECONDS = 5;

    private final int workerId;
    private final WorkUnitExecutor processor;
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean accepting = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public WorkerGrpcService(int workerId) {
        this(workerId, new WorkUnitProcessor(), DEFAULT_COMPUTE_THREADS,
                DEFAULT_QUEUE_CAPACITY);
    }

    WorkerGrpcService(int workerId, WorkUnitExecutor processor,
                      int computeThreads, int queueCapacity) {
        if (workerId <= 0) throw new IllegalArgumentException("worker ID must be positive");
        if (processor == null) throw new IllegalArgumentException("processor must not be null");
        if (computeThreads <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException("executor sizes must be positive");
        }
        this.workerId = workerId;
        this.processor = processor;
        this.executor = new ThreadPoolExecutor(
                computeThreads, computeThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), task -> {
                    Thread thread = new Thread(task, "worker-" + workerId + "-grpc-compute");
                    thread.setDaemon(false);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    public void startAccepting() {
        if (closed.get()) throw new IllegalStateException("worker service is closed");
        accepting.set(true);
    }

    public void stopAccepting() {
        accepting.set(false);
    }

    @Override
    public void executeTask(ExecuteTaskRequest request,
                            StreamObserver<ExecuteTaskResponse> observer) {
        if (!accepting.get()) {
            observer.onError(Status.UNAVAILABLE.withDescription(
                    "worker " + workerId + " is not accepting tasks").asRuntimeException());
            return;
        }

        final WorkUnit unit;
        try {
            unit = WorkerProtoMapper.toWorkUnit(request);
        } catch (IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage())
                    .withCause(e).asRuntimeException());
            return;
        }

        Context context = Context.current();
        try {
            executor.execute(context.wrap(() -> {
                try {
                    if (context.isCancelled() || Thread.currentThread().isInterrupted()) {
                        throw Status.CANCELLED.withDescription("task was cancelled").asRuntimeException();
                    }
                    WorkResult result = processor.execute(unit);
                    if (context.isCancelled()) return;
                    observer.onNext(WorkerProtoMapper.toResponse(request, workerId, result));
                    observer.onCompleted();
                } catch (io.grpc.StatusRuntimeException e) {
                    if (!context.isCancelled()) observer.onError(e);
                } catch (IllegalArgumentException e) {
                    if (!context.isCancelled()) observer.onError(Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage()).withCause(e).asRuntimeException());
                } catch (ArithmeticException e) {
                    if (!context.isCancelled()) observer.onError(Status.OUT_OF_RANGE
                            .withDescription(e.getMessage()).withCause(e).asRuntimeException());
                } catch (Throwable e) {
                    if (!context.isCancelled()) observer.onError(Status.INTERNAL
                            .withDescription("task computation failed")
                            .withCause(e).asRuntimeException());
                }
            }));
        } catch (RejectedExecutionException e) {
            Status status = accepting.get() ? Status.RESOURCE_EXHAUSTED : Status.UNAVAILABLE;
            observer.onError(status.withDescription(accepting.get()
                    ? "worker task queue is full" : "worker is shutting down")
                    .withCause(e).asRuntimeException());
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        stopAccepting();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
