package com.cs324a2.bootstrap;

import com.cs324a2.grpc.bootstrap.WorkerEndpoint;

record WorkerRegistration(WorkerEndpoint endpoint, long lastHeartbeatNanos) {
    WorkerRegistration withHeartbeat(long timestampNanos) {
        return new WorkerRegistration(endpoint, timestampNanos);
    }
}
