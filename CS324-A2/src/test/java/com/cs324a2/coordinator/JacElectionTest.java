package com.cs324a2.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.cs324a2.common.JobRequest;
import com.cs324a2.common.WorkResult;
import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.scheduler.SchedulerConfig;
import com.cs324a2.scheduler.WorkerMembership;
import com.cs324a2.scheduler.WorkerRpcClient;
import com.cs324a2.worker.WorkUnitProcessor;
import com.cs324a2.worker.WorkerProtoMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Covers the Assignment-1 JAC flood election over gRPC:
 * winner selection, exactly-once ELECTION/COORDINATOR handling, agreement,
 * neighbour pruning, bilateral random links, and the five-jobs-per-term
 * rotation.
 */
class JacElectionTest {
    private final List<CoordinatorNode> nodes = new ArrayList<>();

    @AfterEach
    void closeNodes() {
        nodes.forEach(CoordinatorNode::close);
    }

    @Test
    void lowestJacWinsTieGoesToHighestId() {
        assertTrue(CoordinatorNode.isBetterCandidate(1, 0, 2, 3));
        assertFalse(CoordinatorNode.isBetterCandidate(2, 3, 1, 0));
        // Tie: highest worker ID wins.
        assertTrue(CoordinatorNode.isBetterCandidate(9, 2, 4, 2));
        assertFalse(CoordinatorNode.isBetterCandidate(4, 2, 9, 2));
        // Identical candidates are not "better".
        assertFalse(CoordinatorNode.isBetterCandidate(5, 1, 5, 1));
    }

    @Test
    void electionMessageIsProcessedAtMostOnce() {
        CoordinatorNode node = singleNode(21);
        var req = com.cs324a2.grpc.coordinator.ElectionRequest.newBuilder()
                .setElectionId("ELECTION-1-1").setSenderId(99)
                .setSenderHost("localhost").setSenderPort(59999).build();
        var first = node.handlePropagateElection(req);
        assertFalse(first.getDuplicate());
        assertEquals(21, first.getBestWorkerId());
        var second = node.handlePropagateElection(req);
        assertTrue(second.getDuplicate());
    }

    @Test
    void coordinatorMessageIsProcessedAtMostOnceAndStaleTermsRejected() {
        CoordinatorNode node = singleNode(22);
        var announce = com.cs324a2.grpc.coordinator.CoordinatorAnnounce.newBuilder()
                .setCoordinatorMessageId("COORDINATOR-E1").setCoordinatorId(22)
                .setCoordinatorHost("localhost").setSenderId(22)
                .setSenderHost("localhost").setTerm(7).build();
        var first = node.handlePropagateCoordinator(announce);
        assertTrue(first.getAccepted());
        assertEquals(22, node.leaderId());
        var replay = node.handlePropagateCoordinator(announce);
        assertFalse(replay.getAccepted());
        // Stale term with a fresh message id is still rejected.
        var stale = com.cs324a2.grpc.coordinator.CoordinatorAnnounce.newBuilder(announce)
                .setCoordinatorMessageId("COORDINATOR-E2").setTerm(1).build();
        assertFalse(node.handlePropagateCoordinator(stale).getAccepted());
        assertEquals(22, node.leaderId());
    }

    @Test
    void singleNodeElectsItself() {
        CoordinatorNode node = singleNode(23);
        node.initiateElection();
        assertTrue(node.isLeader());
        assertEquals(23, node.leaderId());
    }

    @Test
    void neighbourLinksRejectSelfAndDuplicatesAndPruneDeparted() {
        CoordinatorNode node = singleNode(24);
        assertFalse(node.handleAddNeighbor(24, "localhost", 51924));
        assertTrue(node.handleAddNeighbor(25, "localhost", 51925));
        assertFalse(node.handleAddNeighbor(25, "localhost", 51925));
        assertEquals(1, node.getNeighbors().size());
        // Empty membership means worker 25 has departed: prune drops it.
        node.pruneNeighborsToActive();
        assertTrue(node.getNeighbors().isEmpty());
        assertTrue(node.handleGetState().getNeighborIdsList().isEmpty());
    }

    @Test
    void jacIsExposedInCoordinatorState() {
        CoordinatorNode node = singleNode(26);
        node.setJAC(4);
        var state = node.handleGetState();
        assertEquals(4, state.getJac());
        assertEquals(26, state.getWorkerId());
    }

    @Test
    void threeNodesAgreeOnHighestIdWhenJacTiesAndRotateAfterFiveJobs()
            throws Exception {
        int[] ports = {freePort(), freePort(), freePort()};
        List<WorkerEndpoint> endpoints = List.of(
                endpoint(1, ports[0]), endpoint(2, ports[1]), endpoint(3, ports[2]));
        StaticMembership membership = new StaticMembership(endpoints);
        SimpleRpcClient rpc = new SimpleRpcClient();
        SchedulerConfig config = new SchedulerConfig(Duration.ofSeconds(8),
                Duration.ofSeconds(60), Duration.ofMillis(100), 5, 8, 128);

        CoordinatorNode first = start(1, ports[0], membership, rpc, config);
        CoordinatorNode second = start(2, ports[1], membership, rpc, config);
        CoordinatorNode third = start(3, ports[2], membership, rpc, config);
        List<CoordinatorNode> all = List.of(first, second, third);

        // All JACs are zero, so the highest ID must win and all must agree.
        CoordinatorNode leader = awaitSingleLeader(all, 20_000);
        assertEquals(3, leader.workerId());
        for (CoordinatorNode node : all) {
            assertEquals(3, node.leaderId());
        }
        // Let concurrent opening elections converge on one term before jobs.
        awaitSameTerm(all, 10_000);
        leader = awaitSingleLeader(all, 20_000);
        assertEquals(3, leader.workerId());

        // Bilateral random links: every node must end up with a neighbour.
        first.connectToRandomNeighbor();
        second.connectToRandomNeighbor();
        third.connectToRandomNeighbor();
        for (CoordinatorNode node : all) {
            assertFalse(node.getNeighbors().isEmpty(),
                    "worker " + node.workerId() + " should have a neighbour");
            for (var n : node.getNeighbors()) {
                assertNotEquals(node.workerId(), n.getWorkerId());
            }
        }

        // Five jobs on the leader exhaust the term; leadership must rotate to
        // the lowest JAC (one of the zero-JAC followers, i.e. worker 2).
        for (int i = 1; i <= 5; i++) {
            long result = leader.submitJob(
                    JobRequest.max("jac-term-job-" + i, List.of(3, 7, 1, 9)));
            assertEquals(9L, result);
        }
        assertEquals(5, leader.getJAC());

        CoordinatorNode replacement = awaitLeaderChange(all, leader.workerId(), 20_000);
        assertNotEquals(leader.workerId(), replacement.workerId());
        assertTrue(replacement.getCurrentTerm() > 0);
        assertSingleLeader(all);
    }

    // ---- Helpers ----

    private CoordinatorNode singleNode(int workerId) {
        CoordinatorNode node = new CoordinatorNode(workerId, "localhost",
                52000 + workerId, new EmptyMembership(), new NoopRpcClient(),
                SchedulerConfig.defaults());
        nodes.add(node);
        return node;
    }

    private CoordinatorNode start(int workerId, int port, WorkerMembership membership,
                                  WorkerRpcClient rpc, SchedulerConfig config)
            throws Exception {
        CoordinatorNode node = new CoordinatorNode(workerId, "localhost", port,
                membership, rpc, config);
        nodes.add(node);
        node.start();
        return node;
    }

    private static CoordinatorNode awaitSingleLeader(List<CoordinatorNode> candidates,
                                                     long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<CoordinatorNode> leaders = candidates.stream()
                    .filter(CoordinatorNode::isLeader).toList();
            if (leaders.size() == 1) {
                assertSingleLeader(candidates);
                return leaders.get(0);
            }
            Thread.sleep(50);
        }
        fail("no single leader emerged among "
                + candidates.stream().map(CoordinatorNode::workerId).toList());
        throw new IllegalStateException("unreachable");
    }

    private static CoordinatorNode awaitLeaderChange(List<CoordinatorNode> candidates,
                                                     int deadLeaderId,
                                                     long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<CoordinatorNode> leaders = candidates.stream()
                    .filter(CoordinatorNode::isLeader).toList();
            if (leaders.size() == 1 && leaders.get(0).workerId() != deadLeaderId) {
                return leaders.get(0);
            }
            Thread.sleep(50);
        }
        fail("leadership did not rotate away from " + deadLeaderId);
        throw new IllegalStateException("unreachable");
    }

    private static void assertSingleLeader(List<CoordinatorNode> candidates) {
        long leaders = candidates.stream().filter(CoordinatorNode::isLeader).count();
        assertEquals(1, leaders, "exactly one active coordinator is allowed, found "
                + leaders);
    }

    private static void awaitSameTerm(List<CoordinatorNode> candidates,
                                      long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            long term = candidates.get(0).getCurrentTerm();
            boolean same = candidates.stream()
                    .allMatch(n -> n.getCurrentTerm() == term);
            if (same) return;
            Thread.sleep(50);
        }
        fail("terms did not converge among "
                + candidates.stream().map(CoordinatorNode::workerId).toList());
    }

    private static WorkerEndpoint endpoint(int workerId, int port) {
        return WorkerEndpoint.newBuilder().setWorkerId(workerId).setHost("localhost")
                .setPort(port).setRegistrationId("jac-reg-" + workerId).build();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class EmptyMembership implements WorkerMembership {
        @Override public Snapshot snapshot() { return new Snapshot(0, List.of()); }
        @Override public long awaitChange(long observedVersion, Duration duration) {
            return observedVersion;
        }
        @Override public void close() {}
    }

    private static final class StaticMembership implements WorkerMembership {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private final List<WorkerEndpoint> workers;
        private final long version = 1;

        private StaticMembership(List<WorkerEndpoint> workers) {
            this.workers = List.copyOf(workers);
        }

        @Override public Snapshot snapshot() {
            return new Snapshot(version, workers);
        }

        @Override public long awaitChange(long observedVersion, Duration duration)
                throws InterruptedException {
            lock.lockInterruptibly();
            try {
                if (version == observedVersion) {
                    changed.awaitNanos(duration.toNanos());
                }
                return version;
            } finally {
                lock.unlock();
            }
        }

        @Override public void close() {}
    }

    private static final class NoopRpcClient implements WorkerRpcClient {
        @Override public ExecuteTaskResponse execute(WorkerEndpoint worker,
                                                     ExecuteTaskRequest request,
                                                     Duration timeout) {
            throw new UnsupportedOperationException("no tasks in this test");
        }
        @Override public void retainWorkers(long version,
                                            Collection<WorkerEndpoint> workers) {}
        @Override public void close() {}
    }

    /** Executes tasks for real via the shared processor. */
    private static final class SimpleRpcClient implements WorkerRpcClient {
        private final WorkUnitProcessor processor = new WorkUnitProcessor();

        @Override public ExecuteTaskResponse execute(WorkerEndpoint worker,
                                                     ExecuteTaskRequest request,
                                                     Duration timeout) {
            WorkResult result = processor.execute(
                    WorkerProtoMapper.toWorkUnit(request));
            return WorkerProtoMapper.toResponse(request, worker.getWorkerId(), result);
        }
        @Override public void retainWorkers(long version,
                                            Collection<WorkerEndpoint> workers) {}
        @Override public void close() {}
    }
}
