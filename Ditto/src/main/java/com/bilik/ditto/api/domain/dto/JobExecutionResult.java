package com.bilik.ditto.api.domain.dto;

public record JobExecutionResult (
        String jobId,
        boolean queued,
        boolean executed,
        boolean errorOccured,
        String error
) {

    public static JobExecutionResult error(String error) {
        return new JobExecutionResult(null, false, false, true, error);
    }

    public static JobExecutionResult queued(String jobId) {
        return new JobExecutionResult(jobId, true, false, false, null);
    }

    public static JobExecutionResult executed(String jobId) {
        return new JobExecutionResult(jobId, false, true, false, null);
    }

}
