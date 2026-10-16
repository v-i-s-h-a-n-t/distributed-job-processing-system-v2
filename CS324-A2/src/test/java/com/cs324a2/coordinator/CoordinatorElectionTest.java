package com.cs324a2.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324a2.grpc.bootstrap.WorkerEndpoint;
import com.cs324a2.grpc.worker.ExecuteTaskRequest;
import com.cs324a2.grpc.worker.ExecuteTaskResponse;
import com.cs324a2.scheduler.SchedulerConfig;
import com.cs324a2.scheduler.WorkerMembership;
import com.cs324a2.scheduler.WorkerRpcClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Unit checks for leader terms, single-vote-per-term, and protection against
 * two active coordinators in the same term. No servers are started; the gRPC
 * handlers are exercised directly.
 */
class CoordinatorElectionTest {
    private final List<CoordinatorNode> nodes = new ArrayList<>();

    @AfterEach
    void closeNodes() {
        nodes.forEach(CoordinatorNode::close);
    }

    @Test
    void termsAreMonotonicAndStaleTermsAreRejected() {
        CoordinatorNode node = node(1);
        assertEquals(0, node.getCurrentTerm());

        var granted = node.handleRequestVote(3, 2, Long.MAX_VALUE);
        assertTrue(granted.granted());
        assertEquals(3, granted.term());
        assertEquals(3, node.getCurrentTerm());

        var staleVote = node.handleRequestVote(2, 5, Long.MAX_VALUE);
        assertFalse(staleVote.granted());
        assertEquals(3, staleVote.term());

        var staleHeartbeat = node.handleHeartbeat(1, 5, "localhost", 51999, 0);
        assertFalse(staleHeartbeat.accepted());
        assertEquals(3, node.getCurrentTerm());
    }

    @Test
    void onlyOneVoteIsGrantedPerTerm() {
        CoordinatorNode node = node(1);

        assertTrue(node.handleRequestVote(4, 2, Long.MAX_VALUE).granted());
        assertFalse(node.handleRequestVote(4, 3, Long.MAX_VALUE).granted());
        // Same candidate may be granted again (idempotent retry), still one vote.
        assertTrue(node.handleRequestVote(4, 2, Long.MAX_VALUE).granted());
        // A higher term resets the vote.
        assertTrue(node.handleRequestVote(5, 3, Long.MAX_VALUE).granted());
    }

    @Test
    void staleCandidateWithoutReplicatedStateIsRejected() {
        CoordinatorNode node = node(1);
        // Advance this node's watermark by learning a replicated job first.
        node.handleReplicate(6, 2, 41, jobState("old-job", 41, 6));
        assertEquals(41, node.getProgress());

        // Candidate at term 7 but behind on progress must not win here.
        assertFalse(node.handleRequestVote(7, 3, 12).granted());
        // An up-to-date candidate is accepted.
        assertTrue(node.handleRequestVote(7, 3, 41).granted());
    }

    @Test
    void secondLeaderInSameTermIsRejected() {
        CoordinatorNode node = node(1);

        var first = node.handleHeartbeat(8, 2, "localhost", 51998, 10);
        assertTrue(first.accepted());
        assertEquals(2, node.leaderId());

        var second = node.handleHeartbeat(8, 3, "localhost", 51997, 10);
        assertFalse(second.accepted());
        assertEquals(2, node.leaderId());

        // The recognised leader's next heartbeat is still accepted.
        assertTrue(node.handleHeartbeat(8, 2, "localhost", 51998, 11).accepted());
    }

    @Test
    void higherTermHeartbeatReplacesTheLeader() {
        CoordinatorNode node = node(1);
        assertTrue(node.handleHeartbeat(8, 2, "localhost", 51998, 3).accepted());
        assertTrue(node.handleHeartbeat(9, 3, "localhost", 51997, 4).accepted());
        assertEquals(9, node.getCurrentTerm());
        assertEquals(3, node.leaderId());
    }

    @Test
    void replicationFromNonLeaderOfTermIsRejected() {
        CoordinatorNode node = node(1);
        assertTrue(node.handleHeartbeat(10, 2, "localhost", 51998, 0).accepted());
        var rogue = node.handleReplicate(10, 3, 99, jobState("rogue-job", 99, 10));
        assertFalse(rogue.accepted());
        assertTrue(node.jobs().isEmpty());
    }

    @Test
    void betterAnnouncedLeaderHeartbeatReconcilesSameTermBeforeReplication() {
        CoordinatorNode node = node(1);
        assertTrue(node.handleHeartbeat(10, 2, "localhost", 51998, 0, 4).accepted());
        // Concurrent election floods can announce a more current winner in
        // the same term. The follower must follow it before its checkpoint.
        assertTrue(node.handleHeartbeat(10, 3, "localhost", 51999, 1, 0).accepted());
        assertEquals(3, node.leaderId());
        assertTrue(node.handleReplicate(10, 3, 1,
                jobState("elected-job", 1, 10)).accepted());
        assertEquals(1, node.jobs().size());
    }

    @Test
    void heartbeatProgressDoesNotMakeEmptyReplicaEligibleAsCurrent() {
        CoordinatorNode node = node(1);
        assertTrue(node.handleHeartbeat(10, 2, "localhost", 51998, 99, 0).accepted());
        assertEquals(0, node.getProgress());
    }

    private CoordinatorNode node(int workerId) {
        CoordinatorNode node = new CoordinatorNode(workerId, "localhost",
                51990 + workerId, new EmptyMembership(), new NoopRpcClient(),
                SchedulerConfig.defaults());
        nodes.add(node);
        return node;
    }

    private static com.cs324a2.grpc.coordinator.JobStateProto jobState(
            String jobId, long progress, long term) {
        return com.cs324a2.grpc.coordinator.JobStateProto.newBuilder()
                .setJob(com.cs324a2.grpc.coordinator.JobSpec.newBuilder()
                        .setJobId(jobId)
                        .setOperation(com.cs324a2.grpc.coordinator.Operation.MAX)
                        .addNumbers(1).setHasRange(false))
                .setStatus(com.cs324a2.grpc.coordinator.JobStatus.JOB_ACTIVE)
                .setProgressIndex(progress).setTerm(term).build();
    }

    private static final class EmptyMembership implements WorkerMembership {
        @Override public Snapshot snapshot() { return new Snapshot(0, List.of()); }
        @Override public long awaitChange(long observedVersion, Duration duration) {
            return observedVersion;
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
}
