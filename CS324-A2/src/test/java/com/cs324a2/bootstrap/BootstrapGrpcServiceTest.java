package com.cs324a2.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a2.grpc.bootstrap.MembershipEvent;
import com.cs324a2.grpc.coordinator.CoordinatorServiceGrpc;
import com.cs324a2.grpc.coordinator.PingResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.time.Duration;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BootstrapGrpcServiceTest {
    private BootstrapGrpcService service;
    private Server server;
    private BootstrapClient client;

    @BeforeEach
    void startServer() throws Exception {
        service = new BootstrapGrpcService(600, 30);
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(service)
                .build()
                .start();
        var channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();
        client = new BootstrapClient(channel);
    }

    @AfterEach
    void stopServer() throws Exception {
        if (client != null) client.close();
        if (service != null) service.close();
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void registrationHeartbeatAndDeregistrationUseRegistrationId() {
        BootstrapClient.Registration registration = client.register(1, "localhost", 51001);
        assertEquals(1, registration.activeWorkers().size());
        assertTrue(client.heartbeat(1, registration.registrationId()));
        assertFalse(client.heartbeat(1, "old-registration"));
        assertFalse(client.deregister(1, "old-registration"));
        assertEquals(1, client.listWorkers().activeWorkers().size());
        assertTrue(client.deregister(1, registration.registrationId()));
        assertTrue(client.listWorkers().activeWorkers().isEmpty());
    }

    @Test
    void rejectsDuplicateLiveWorkerId() {
        client.register(2, "localhost", 51002);
        try {
            client.register(2, "localhost", 51003);
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.ALREADY_EXISTS, e.getStatus().getCode());
            return;
        }
        throw new AssertionError("duplicate worker ID should be rejected");
    }

    @Test
    void twoTransportFailuresPruneOnlyTheMatchingRegistration() {
        BootstrapClient.Registration registration = client.register(8, "localhost", 51008);
        var endpoint = client.listWorkers().activeWorkers().get(0);
        var stale = endpoint.toBuilder().setRegistrationId("stale").build();
        assertFalse(client.reportWorkerRpcResult(stale, false));
        assertFalse(client.reportWorkerRpcResult(endpoint, false));
        assertEquals(1, client.listWorkers().activeWorkers().size());
        client.reportWorkerRpcResult(endpoint, true);
        assertFalse(client.reportWorkerRpcResult(endpoint, false));
        assertTrue(client.reportWorkerRpcResult(endpoint, false));
        assertTrue(client.listWorkers().activeWorkers().isEmpty());
        assertFalse(client.heartbeat(8, registration.registrationId()));
    }

    @Test
    void reporterCannotRemoveWorkerReachableFromBootstrap() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Server worker = ServerBuilder.forPort(port).addService(
                new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override public void ping(
                            com.cs324a2.grpc.coordinator.PingRequest request,
                            StreamObserver<PingResponse> observer) {
                        observer.onNext(PingResponse.newBuilder().setWorkerId(9).build());
                        observer.onCompleted();
                    }
                }).build().start();
        try {
            client.register(9, "localhost", port);
            var endpoint = client.listWorkers().activeWorkers().get(0);
            assertFalse(client.reportWorkerRpcResult(endpoint, false));
            assertFalse(client.reportWorkerRpcResult(endpoint, false));
            assertEquals(1, client.listWorkers().activeWorkers().size());
        } finally {
            worker.shutdownNow();
        }
    }

    @Test
    void watchSendsSnapshotThenJoinAndLeaveEvents() throws Exception {
        BootstrapClient.Registration first = client.register(3, "localhost", 51003);
        BlockingQueue<MembershipEvent> events = new LinkedBlockingQueue<>();
        BlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
        BootstrapClient.WatchHandle watch = client.watchMembership(0,
                queueingObserver(events, errors));

        try {
            MembershipEvent snapshot = takeEvent(events, errors);
            assertEquals(MembershipEvent.Type.SNAPSHOT, snapshot.getType());
            assertEquals(first.registrationId(),
                    snapshot.getActiveWorkers(0).getRegistrationId());

            BootstrapClient.Registration second = client.register(4, "localhost", 51004);
            MembershipEvent joined = takeEvent(events, errors);
            assertEquals(MembershipEvent.Type.WORKER_JOINED, joined.getType());
            assertEquals(4, joined.getWorker().getWorkerId());

            assertTrue(client.deregister(4, second.registrationId()));
            MembershipEvent left = takeEvent(events, errors);
            assertEquals(MembershipEvent.Type.WORKER_LEFT, left.getType());
            assertEquals(4, left.getWorker().getWorkerId());
        } finally {
            watch.cancel();
        }
    }

    @Test
    void missedHeartbeatsExpireAndPublishWorkerLeft() throws Exception {
        BlockingQueue<MembershipEvent> events = new LinkedBlockingQueue<>();
        BlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
        BootstrapClient.WatchHandle watch = client.watchMembership(0,
                queueingObserver(events, errors));
        try {
            assertEquals(MembershipEvent.Type.SNAPSHOT,
                    takeEvent(events, errors).getType());
            client.register(5, "localhost", 51005);

            MembershipEvent joined = takeEvent(events, errors);
            assertEquals(MembershipEvent.Type.WORKER_JOINED, joined.getType());

            MembershipEvent left = takeEvent(events, errors);
            assertEquals(MembershipEvent.Type.WORKER_LEFT, left.getType());
            assertEquals(5, left.getWorker().getWorkerId());
            assertTrue(client.listWorkers().activeWorkers().isEmpty());
        } finally {
            watch.cancel();
        }
    }

    private static StreamObserver<MembershipEvent> queueingObserver(
            BlockingQueue<MembershipEvent> events,
            BlockingQueue<Throwable> errors) {
        return new StreamObserver<>() {
            @Override
            public void onNext(MembershipEvent event) {
                events.add(event);
            }

            @Override
            public void onError(Throwable error) {
                errors.add(error);
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private static MembershipEvent takeEvent(
            BlockingQueue<MembershipEvent> events,
            BlockingQueue<Throwable> errors) throws InterruptedException {
        MembershipEvent event = events.poll(Duration.ofSeconds(3).toMillis(),
                TimeUnit.MILLISECONDS);
        if (event != null) return event;
        Throwable error = errors.poll();
        if (error != null) throw new AssertionError("membership watch failed", error);
        throw new AssertionError("timed out waiting for membership event");
    }
}
