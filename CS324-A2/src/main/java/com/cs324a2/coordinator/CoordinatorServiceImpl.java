package com.cs324a2.coordinator;

import com.cs324a2.grpc.coordinator.CoordinatorAck;
import com.cs324a2.grpc.coordinator.CoordinatorAnnounce;
import com.cs324a2.grpc.coordinator.CoordinatorJobRequest;
import com.cs324a2.grpc.coordinator.CoordinatorJobResponse;
import com.cs324a2.grpc.coordinator.CoordinatorServiceGrpc;
import com.cs324a2.grpc.coordinator.ElectionRequest;
import com.cs324a2.grpc.coordinator.ElectionResponse;
import com.cs324a2.grpc.coordinator.GetCoordinatorStateRequest;
import com.cs324a2.grpc.coordinator.GetCoordinatorStateResponse;
import com.cs324a2.grpc.coordinator.JobProgressRequest;
import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import com.cs324a2.grpc.coordinator.LeaderHeartbeatRequest;
import com.cs324a2.grpc.coordinator.LeaderHeartbeatResponse;
import com.cs324a2.grpc.coordinator.NeighborRequest;
import com.cs324a2.grpc.coordinator.NeighborResponse;
import com.cs324a2.grpc.coordinator.PingRequest;
import com.cs324a2.grpc.coordinator.PingResponse;
import com.cs324a2.grpc.coordinator.ReplicateJobStateRequest;
import com.cs324a2.grpc.coordinator.ReplicateJobStateResponse;
import com.cs324a2.grpc.coordinator.RequestVoteRequest;
import com.cs324a2.grpc.coordinator.RequestVoteResponse;
import io.grpc.stub.StreamObserver;

/**
 * gRPC boundary for the coordinator contract. All election RPCs travel here.
 * Each handler delegates to the owning {@link CoordinatorNode}, which
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
                request.getCommitProgressIndex(), request.getLeaderJac());
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

    @Override
    public void subscribeJobProgress(JobProgressRequest request,
                                     StreamObserver<JobProgressUpdate> observer) {
        node.subscribeJobProgress(request, observer);
    }

    @Override
    public void addNeighbor(NeighborRequest request,
                            StreamObserver<NeighborResponse> observer) {
        boolean added = node.handleAddNeighbor(
                request.getWorkerId(), request.getHost(), request.getPort());
        observer.onNext(NeighborResponse.newBuilder()
                .setAdded(added).setNeighborCount(node.getNeighbors().size()).build());
        observer.onCompleted();
    }

    @Override
    public void propagateElection(ElectionRequest request,
                                  StreamObserver<ElectionResponse> observer) {
        observer.onNext(node.handlePropagateElection(request));
        observer.onCompleted();
    }

    @Override
    public void propagateCoordinator(CoordinatorAnnounce request,
                                     StreamObserver<CoordinatorAck> observer) {
        observer.onNext(node.handlePropagateCoordinator(request));
        observer.onCompleted();
    }

    @Override
    public void ping(PingRequest request,
                     StreamObserver<PingResponse> observer) {
        observer.onNext(node.handlePing(request.getCallerId()));
        observer.onCompleted();
    }
}
