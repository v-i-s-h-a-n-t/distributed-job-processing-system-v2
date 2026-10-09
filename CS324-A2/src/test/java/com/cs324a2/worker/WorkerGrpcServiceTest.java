package com.cs324a2.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a2.common.WorkUnit;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.grpc.worker.NumberPayload;
import com.cs324a2.grpc.worker.Operation;
import com.cs324a2.grpc.worker.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.Server;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class WorkerGrpcServiceTest {
    private WorkerGrpcService service;
    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void close() throws Exception {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        if (service != null) service.close();
    }

    @Test
    void executesAllOperationsAndPreservesIdentity() throws Exception {
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = start(7, new WorkUnitProcessor(), 4, 16);

        ExecuteTaskResponse max = stub.executeTask(WorkerProtoMapper.toRequest(
                "max-task", 2, WorkUnit.max("job-max", 3, List.of(-8, -2, -5))));
        ExecuteTaskResponse sum = stub.executeTask(WorkerProtoMapper.toRequest(
                "sum-task", 1, WorkUnit.primeSum("job-sum", 0, 1, 20)));
        ExecuteTaskResponse count = stub.executeTask(WorkerProtoMapper.toRequest(
                "count-task", 1, WorkUnit.primeCount("job-count", 1,
                        List.of(2, 2, 4, 5, 8))));

        assertEquals(-2, max.getValue());
        assertEquals("max-task", max.getTaskId());
        assertEquals("job-max", max.getJobId());
        assertEquals(3, max.getPartitionId());
        assertEquals(2, max.getAttempt());
        assertEquals(7, max.getWorkerId());
        assertEquals(77, sum.getValue());
        assertEquals(3, count.getValue());
    }

    @Test
    void rejectsInvalidRequestsWithInvalidArgument() throws Exception {
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = start(1, new WorkUnitProcessor(), 1, 4);
        ExecuteTaskRequest invalid = ExecuteTaskRequest.newBuilder()
                .setTaskId("task").setJobId("job").setAttempt(1)
                .setOperation(Operation.PRIMESUM)
                .setNumberPayload(NumberPayload.newBuilder().addNumbers(2)).build();

        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> stub.executeTask(invalid));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void mapsOverflowAndUnexpectedComputationFailures() throws Exception {
        WorkerServiceGrpc.WorkerServiceBlockingStub overflowStub = start(1,
                unit -> { throw new ArithmeticException("overflow"); }, 1, 4);
        ExecuteTaskRequest request = WorkerProtoMapper.toRequest(
                "task", 1, WorkUnit.max("job", 0, List.of(1)));
        StatusRuntimeException overflow = assertThrows(StatusRuntimeException.class,
                () -> overflowStub.executeTask(request));
        assertEquals(Status.Code.OUT_OF_RANGE, overflow.getStatus().getCode());

        channel.shutdownNow();
        server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        service.close();
        channel = null;
        server = null;
        service = null;

        WorkerServiceGrpc.WorkerServiceBlockingStub failingStub = start(1,
                unit -> { throw new IllegalStateException("broken"); }, 1, 4);
        StatusRuntimeException unexpected = assertThrows(StatusRuntimeException.class,
                () -> failingStub.executeTask(request));
        assertEquals(Status.Code.INTERNAL, unexpected.getStatus().getCode());
    }

    @Test
    void executesTasksConcurrently() throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        WorkUnitExecutor blocking = unit -> {
            bothStarted.countDown();
            try {
                if (!release.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("not released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return new WorkUnitProcessor().execute(unit);
        };
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = start(2, blocking, 2, 4);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<ExecuteTaskResponse> first = callers.submit(() -> stub.executeTask(
                    WorkerProtoMapper.toRequest("a", 1, WorkUnit.max("a", 0, List.of(4)))));
            Future<ExecuteTaskResponse> second = callers.submit(() -> stub.executeTask(
                    WorkerProtoMapper.toRequest("b", 1, WorkUnit.max("b", 0, List.of(9)))));
            assertTrue(bothStarted.await(2, TimeUnit.SECONDS));
            release.countDown();
            assertEquals(4, first.get(2, TimeUnit.SECONDS).getValue());
            assertEquals(9, second.get(2, TimeUnit.SECONDS).getValue());
        } finally {
            release.countDown();
            callers.shutdownNow();
        }
    }

    @Test
    void stoppedWorkerRejectsNewTasks() throws Exception {
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = start(3, new WorkUnitProcessor(), 1, 4);
        service.stopAccepting();
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> stub.executeTask(WorkerProtoMapper.toRequest(
                        "task", 1, WorkUnit.max("job", 0, List.of(1)))));
        assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub start(
            int workerId, WorkUnitExecutor executor, int threads, int queue) throws Exception {
        String name = InProcessServerBuilder.generateName();
        service = new WorkerGrpcService(workerId, executor, threads, queue);
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        service.startAccepting();
        return WorkerServiceGrpc.newBlockingStub(channel);
    }
}
