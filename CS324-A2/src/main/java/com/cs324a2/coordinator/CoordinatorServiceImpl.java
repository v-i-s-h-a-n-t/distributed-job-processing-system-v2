package com.cs324a2.coordinator;

import com.cs324a2.grpc.coordinator.CoordinatorJobRequest;
import com.cs324a2.grpc.coordinator.CoordinatorJobResponse;
import com.cs324a2.grpc.coordinator.CoordinatorServiceGrpc;
import com.cs324a2.grpc.coordinator.GetCoordinatorStateRequest;
import com.cs324a2.grpc.coordinator.GetCoordinatorStateResponse;
import com.cs324a2.grpc.coordinator.LeaderHeartbeatRequest;
import com.cs324a2.grpc.coordinator.LeaderHeartbeatResponse;
import com.cs324a2.grpc.coordinator.ReplicateJobStateRequest;
import com.cs324a2.grpc.coordinator.ReplicateJobStateResponse;
import com.cs324a2.grpc.coordinator.RequestVoteRequest;
import com.cs324a2.grpc.coordinator.RequestVoteResponse;
import io.grpc.stub.StreamObserver;

/**
 * gRPC boundary for the coordinator contract. All election RPCs travel here,
 * replacing the legacy RMI {@code propagateElection / propagateCoordinator}
 * path. Each handler delegates to the owning {@link CoordinatorNode}, which
 * owns terms, votes, heartbeats, replication, and job execution.
 */
final class CoordinatorServiceImpl
        extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
    private final CoordinatorNode node;

    CoordinatorServiceImpl(CoordinatorNode node) {
        if (node == null) throw new IllegalArgumentException("node must not be null");
        this.node = node;
    }

    @Override
    public void requestVote(RequestVoteRequest request,
                            StreamObserver<RequestVoteResponse> observer) {
        CoordinatorNode.VoteOutcome outcome = node.handleRequestVote(
                request.getTerm(), request.getCandidateId(),
                request.getLastProgressIndex());
        observer.onNext(RequestVoteResponse.newBuilder()
                .setTerm(outcome.term())
                .setVoteGranted(outcome.granted())
                .setVoterId(node.workerId())
                .build());
        observer.onCompleted();
    }

    @Override
    public void leaderHeartbeat(LeaderHeartbeatRequest request,
                                StreamObserver<LeaderHeartbeatResponse> observer) {
        CoordinatorNode.HeartbeatOutcome outcome = node.handleHeartbeat(
                request.getTerm(), request.getLeaderId(),
                request.getLeaderHost(), request.getLeaderPort(),
                request.getCommitProgressIndex());
        observer.onNext(LeaderHeartbeatResponse.newBuilder()
                .setTerm(outcome.term())
                .setSuccess(outcome.accepted())
                .setFollowerId(node.workerId())
                .build());
        observer.onCompleted();
    }

    @Override
    public void replicateJobState(ReplicateJobStateRequest request,
                                  StreamObserver<ReplicateJobStateResponse> observer) {
        CoordinatorNode.ReplicateOutcome outcome = node.handleReplicate(
                request.getTerm(), request.getLeaderId(),
                request.getProgressIndex(), request.getJobState());
        observer.onNext(ReplicateJobStateResponse.newBuilder()
                .setTerm(outcome.term())
                .setSuccess(outcome.accepted())
                .setFollowerId(node.workerId())
                .build());
        observer.onCompleted();
    }

    @Override
    public void submitJob(CoordinatorJobRequest request,
                          StreamObserver<CoordinatorJobResponse> observer) {
        observer.onNext(node.handleSubmit(request.getJob()));
        observer.onCompleted();
    }

    @Override
    public void getCoordinatorState(GetCoordinatorStateRequest request,
                                    StreamObserver<GetCoordinatorStateResponse> observer) {
        observer.onNext(node.handleGetState());
        observer.onCompleted();
    }
}
