package com.bilik.ditto.gateway.dto;

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

    public AvailableSlots merge(AvailableSlots availableSlots) {
        return new AvailableSlots(
                this.executing + availableSlots.executing,
                this.executionCapacity + availableSlots.executionCapacity,
                this.availableExecutionSlots + availableSlots.availableExecutionSlots,
                this.waiting + availableSlots.waiting,
                this.waitingQueueSize + availableSlots.waitingQueueSize,
                this.availableQueueSlots + availableSlots.availableQueueSlots
                );
    }
}
