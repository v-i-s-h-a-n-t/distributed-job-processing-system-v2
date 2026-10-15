package com.cs324a2.coordinator;

import com.cs324a2.grpc.coordinator.CoordinatorAck;
import com.cs324a2.grpc.coordinator.CoordinatorAnnounce;
import com.cs324a2.grpc.coordinator.CoordinatorJobRequest;
import com.cs324a2.grpc.coordinator.CoordinatorJobResponse;
import com.cs324a2.grpc.coordinator.GetCoordinatorStateRequest;
import com.cs324a2.grpc.coordinator.GetCoordinatorStateResponse;
import com.cs324a2.grpc.coordinator.CoordinatorServiceGrpc;
import com.cs324a2.grpc.coordinator.ElectionRequest;
import com.cs324a2.grpc.coordinator.ElectionResponse;
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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * gRPC client for coordinator-to-coordinator RPCs (votes, heartbeats,
 * replication). Channels are cached per host:port and closed on {@link #close()}.
 */
final class CoordinatorRpcClient implements AutoCloseable {
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private volatile boolean closed;

    private ManagedChannel channelFor(String host, int port) {
        String key = host + ":" + port;
        return channels.computeIfAbsent(key, ignored ->
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    RequestVoteResponse requestVote(String host, int port, RequestVoteRequest request,
                                    long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .requestVote(request);
    }

    LeaderHeartbeatResponse heartbeat(String host, int port, LeaderHeartbeatRequest request,
                                      long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .leaderHeartbeat(request);
    }

    ReplicateJobStateResponse replicate(String host, int port,
                                        ReplicateJobStateRequest request,
                                        long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .replicateJobState(request);
    }

    CoordinatorJobResponse submit(String host, int port,
                                  CoordinatorJobRequest request,
                                  long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .submitJob(request);
    }

    GetCoordinatorStateResponse getState(String host, int port, long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .getCoordinatorState(GetCoordinatorStateRequest.getDefaultInstance());
    }

    NeighborResponse addNeighbor(String host, int port, NeighborRequest request,
                                 long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .addNeighbor(request);
    }

    ElectionResponse propagateElection(String host, int port, ElectionRequest request,
                                       long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .propagateElection(request);
    }

    CoordinatorAck propagateCoordinator(String host, int port, CoordinatorAnnounce request,
                                        long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .propagateCoordinator(request);
    }

    PingResponse ping(String host, int port, int callerId, long timeoutMillis) {
        return stubFor(host, port).withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .ping(PingRequest.newBuilder().setCallerId(callerId).build());
    }

    private CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stubFor(String host, int port) {
        if (closed) throw new IllegalStateException("coordinator RPC client is closed");
        return CoordinatorServiceGrpc.newBlockingStub(channelFor(host, port));
    }

    @Override
    public void close() {
        closed = true;
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdownNow();
            } catch (RuntimeException ignored) {
                // Best effort during shutdown.
            }
        }
        channels.clear();
    }
}
