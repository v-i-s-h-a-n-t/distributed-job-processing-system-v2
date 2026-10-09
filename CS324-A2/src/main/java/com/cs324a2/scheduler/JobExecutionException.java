package com.cs324a2.scheduler;

public final class JobExecutionException extends Exception {
    public JobExecutionException(String message) {
        super(message);
    }

    public JobExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
