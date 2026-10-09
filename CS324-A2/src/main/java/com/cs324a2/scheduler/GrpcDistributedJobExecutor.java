package com.cs324a2.scheduler;

import com.cs324a2.common.JobRequest;

/** Production wiring for Bootstrap membership, worker channels, and scheduling. */
public final class GrpcDistributedJobExecutor implements DistributedJobExecutor {
    private final BootstrapWorkerMembership membership;
    private final DistributedTaskScheduler scheduler;

    public GrpcDistributedJobExecutor(String bootstrapHost, int bootstrapPort,
                                      SchedulerConfig config) {
        membership = new BootstrapWorkerMembership(bootstrapHost, bootstrapPort);
        membership.start();
        scheduler = new DistributedTaskScheduler(
                membership, new GrpcWorkerRpcClient(), config);
    }

    @Override
    public long execute(JobRequest request) throws JobExecutionException {
        return scheduler.execute(request);
    }

    @Override
    public void close() {
        scheduler.close();
    }
}
