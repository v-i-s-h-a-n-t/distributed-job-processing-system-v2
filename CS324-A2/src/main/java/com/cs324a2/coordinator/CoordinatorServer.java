package com.cs324a2.coordinator;

import com.cs324a2.bootstrap.WorkerHeartbeat;
import com.cs324a2.scheduler.BootstrapWorkerMembership;
import com.cs324a2.scheduler.GrpcWorkerRpcClient;
import com.cs324a2.scheduler.SchedulerConfig;

/**
 * Production entry point for a combined worker + coordinator node.
 *
 * <p>Each process serves task execution and the gRPC {@code CoordinatorService}
 * (JAC flood election, leader heartbeats, replicated job checkpoints) on one
 * port, registers with Bootstrap for membership/quorum, and dispatches tasks
 * to peers over gRPC. Kill any process (including the leader) mid-job: the
 * survivors elect a replacement, restore the replicated state, resume
 * unfinished tasks, and keep accepting later jobs.
 *
 * <p>Note: this is the election-capable worker. The task-only
 * {@code com.cs324a2.worker.WorkerServer} executes jobs but never stands for
 * election (no JAC, no CoordinatorService).
 *
 * <p>Usage: {@code CoordinatorServer <workerId> <advertisedHost> <workerPort>
 * <bootstrapHost> <bootstrapPort>}
 */
public final class CoordinatorServer {
    private CoordinatorServer() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: CoordinatorServer <workerId> <advertisedHost> "
                    + "<workerPort> <bootstrapHost> <bootstrapPort>");
            System.exit(1);
        }
        int workerId = Integer.parseInt(args[0]);
        String advertisedHost = args[1];
        int workerPort = Integer.parseInt(args[2]);
        String bootstrapHost = args[3];
        int bootstrapPort = Integer.parseInt(args[4]);

        WorkerHeartbeat heartbeat = new WorkerHeartbeat(workerId, advertisedHost,
                workerPort, bootstrapHost, bootstrapPort);
        BootstrapWorkerMembership membership =
                new BootstrapWorkerMembership(bootstrapHost, bootstrapPort);
        GrpcWorkerRpcClient taskClient = new GrpcWorkerRpcClient();
        CoordinatorNode node = new CoordinatorNode(workerId, advertisedHost,
                workerPort, membership, taskClient, SchedulerConfig.defaults());

        heartbeat.start();
        membership.start();
        node.start();
        // Unstructured overlay: link to one random active worker (bilateral).
        try {
            node.connectToRandomNeighbor();
        } catch (RuntimeException e) {
            System.err.printf("Coordinator node %d random link failed: %s%n",
                    workerId, e.getMessage());
        }
        System.out.printf("Coordinator node %d ready at %s:%d; election runs automatically.%n",
                workerId, advertisedHost, workerPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            node.close();
            membership.close();
            heartbeat.close();
            taskClient.close();
        }, "coordinator-server-shutdown"));

        Thread.currentThread().join();
    }
}
