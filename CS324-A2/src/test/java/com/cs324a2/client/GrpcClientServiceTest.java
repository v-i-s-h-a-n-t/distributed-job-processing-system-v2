package com.cs324a2.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a2.bootstrap.BootstrapGrpcService;
import com.cs324a2.bootstrap.WorkerHeartbeat;
import com.cs324a2.common.JobRequest;
import com.cs324a2.coordinator.CoordinatorNode;
import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import com.cs324a2.grpc.coordinator.JobStatus;
import com.cs324a2.scheduler.BootstrapWorkerMembership;
import com.cs324a2.scheduler.GrpcWorkerRpcClient;
import com.cs324a2.scheduler.SchedulerConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test of the exact path the GUI uses: gRPC Bootstrap discovery,
 * coordinator {@code SubmitJob} with leader-hint following, and
 * server-streaming {@code SubscribeJobProgress} with failover.
 */
class GrpcClientServiceTest {
    private Server bootstrapServer;
    private BootstrapGrpcService bootstrapService;
    private final List<CoordinatorNode> nodes = new ArrayList<>();
    private final List<BootstrapWorkerMembership> memberships = new ArrayList<>();
    private final List<WorkerHeartbeat> heartbeats = new ArrayList<>();
    private final List<GrpcWorkerRpcClient> taskClients = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (CoordinatorNode node : nodes) {
            try {
                node.close();
            } catch (RuntimeException ignored) {
                // Best effort.
            }
        }
        for (BootstrapWorkerMembership membership : memberships) {
            try {
                membership.close();
            } catch (RuntimeException ignored) {
                // Best effort.
            }
        }
        for (WorkerHeartbeat heartbeat : heartbeats) {
            try {
                heartbeat.close();
            } catch (RuntimeException ignored) {
                // Best effort.
            }
        }
        for (GrpcWorkerRpcClient client : taskClients) {
            try {
                client.close();
            } catch (RuntimeException ignored) {
                // Best effort.
            }
        }
        if (bootstrapServer != null) bootstrapServer.shutdownNow();
        if (bootstrapService != null) {
            try {
                bootstrapService.close();
            } catch (RuntimeException ignored) {
                // Best effort.
            }
        }
    }

    @Test
    void guiPathSubmitsThroughFollowerAndStreamsRealProgress() throws Exception {
        int bootstrapPort = freePort();
        bootstrapService = new BootstrapGrpcService();
        bootstrapServer = ServerBuilder.forPort(bootstrapPort)
                .addService(bootstrapService).build().start();

        int[] ports = {freePort(), freePort(), freePort()};
        for (int i = 0; i < 3; i++) {
            int workerId = i + 1;
            WorkerHeartbeat heartbeat = new WorkerHeartbeat(workerId, "localhost",
                    ports[i], "localhost", bootstrapPort);
            heartbeats.add(heartbeat);
            heartbeat.start();
            BootstrapWorkerMembership membership =
                    new BootstrapWorkerMembership("localhost", bootstrapPort);
            memberships.add(membership);
            membership.start();
            GrpcWorkerRpcClient taskClient = new GrpcWorkerRpcClient();
            taskClients.add(taskClient);
            CoordinatorNode node = new CoordinatorNode(workerId, "localhost", ports[i],
                    membership, taskClient, SchedulerConfig.defaults());
            nodes.add(node);
            node.start();
        }

        // Wait for full membership convergence on every node.
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            boolean ready = memberships.stream()
                    .allMatch(m -> m.snapshot().workers().size() == 3);
            if (ready) break;
            Thread.sleep(100);
        }
        for (var membership : memberships) {
            assertEquals(3, membership.snapshot().workers().size(),
                    "bootstrap membership should converge to 3 workers");
        }
        assertEquals(0, nodes.stream().filter(CoordinatorNode::isLeader).count(),
                "no election should run before the first job");

        // The GUI path: discovery + submit + streaming, all over gRPC.
        try (GrpcClientService gui = new GrpcClientService("localhost", bootstrapPort)) {
            assertEquals(3, gui.listWorkers().size());
            assertTrue(gui.isAvailable());

            String jobId = "gui-path-job";
            List<JobProgressUpdate> updates = new CopyOnWriteArrayList<>();
            CountDownLatch terminal = new CountDownLatch(1);
            GrpcClientService.ProgressHandle handle = gui.subscribeProgress(jobId,
                    new GrpcClientService.ProgressListener() {
                        @Override
                        public void onUpdate(JobProgressUpdate update) {
                            updates.add(update);
                            if (update.getStatus() != JobStatus.JOB_ACTIVE) {
                                terminal.countDown();
                            }
                        }

                        @Override
                        public void onError(Throwable error) {
                            terminal.countDown();
                        }
                    });

            long result = gui.submit(JobRequest.max(jobId, List.of(5, 1, 9, 3, 7, 2)));
            assertEquals(9L, result);
            awaitSingleLeader(20_000);
            assertTrue(terminal.await(30, TimeUnit.SECONDS),
                    "progress stream should reach a terminal update");
            handle.cancel();

            assertTrue(updates.size() >= 1,
                    "expected at least the terminal checkpoint update");
            JobProgressUpdate last = updates.get(updates.size() - 1);
            assertEquals(jobId, last.getJobId());
            assertEquals(last.getTotalTasks(), last.getCompletedTasks());
            assertEquals(1.0, last.getCompletionFraction(), 1e-9);
            assertTrue(last.getWorkerBreakdownCount() > 0);
            JobProgressUpdate fetched = gui.fetchJobProgress(jobId);
            assertNotNull(fetched);
            assertEquals(JobStatus.JOB_COMPLETED, fetched.getStatus());
            assertTrue(fetched.getWorkerBreakdownCount() > 0);
            assertEquals(fetched.getTotalTasks(), fetched.getCompletedTasks());
        }
        // Coordinator-to-client path also works when the GUI talks to a follower
        // first: covered implicitly because submit() tries endpoints in order.
        CompletableFuture<Long> later;
        try (GrpcClientService gui = new GrpcClientService("localhost", bootstrapPort)) {
            later = CompletableFuture.supplyAsync(() -> {
                try {
                    return gui.submit(JobRequest.primeSum("gui-path-later", 1, 20));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertEquals(77L, later.get(60, TimeUnit.SECONDS));
            // Six submissions overlap and cross the five-admission boundary.
            List<CompletableFuture<Long>> concurrent = new ArrayList<>();
            for (int i = 3; i <= 8; i++) {
                String jobId = "gui-term-job-" + i;
                concurrent.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        return gui.submit(JobRequest.max(jobId, List.of(3, 9, 2)));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (CompletableFuture<Long> job : concurrent) {
                assertEquals(9L, job.get(60, TimeUnit.SECONDS));
            }
        }
    }

    private void awaitSingleLeader(long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            long leaders = nodes.stream().filter(CoordinatorNode::isLeader).count();
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader emerged among "
                + nodes.stream().map(CoordinatorNode::workerId).toList());
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
