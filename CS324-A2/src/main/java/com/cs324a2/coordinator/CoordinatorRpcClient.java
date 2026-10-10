package com.cs324a2.coordinator;

import com.cs324a2.grpc.coordinator.CoordinatorJobRequest;
import com.cs324a2.grpc.coordinator.CoordinatorJobResponse;
import com.cs324a2.grpc.coordinator.CoordinatorServiceGrpc;
import com.cs324a2.grpc.coordinator.LeaderHeartbeatRequest;
import com.cs324a2.grpc.coordinator.LeaderHeartbeatResponse;
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
