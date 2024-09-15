package com.bilik.ditto.api.domain.dto;

public record AvailableSlots(
        int executing,
        int executionCapacity,
        int availableExecutionSlots,
        int waiting,
        int waitingQueueSize,
        int availableQueueSlots
) {
    public AvailableSlots(int executing,
                          int executionCapacity,
                          int waiting,
                          int waitingQueueSize,
                          int availableQueueSlots) {
        this(
                executing,
                executionCapacity,
                executionCapacity - executing,
                waiting,
                waitingQueueSize,
                availableQueueSlots
        );
    }
}
