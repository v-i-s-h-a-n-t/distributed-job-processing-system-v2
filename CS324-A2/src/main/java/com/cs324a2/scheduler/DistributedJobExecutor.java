package com.cs324a2.scheduler;

import com.cs324a2.common.JobRequest;

/** Small integration seam for the elected coordinator owned by Rohan. */
public interface DistributedJobExecutor extends AutoCloseable {
    long execute(JobRequest request) throws JobExecutionException;

    @Override
    void close();
}
