package com.bilik.ditto.gateway.dto;

public record WorkerEventDto(
        String workerType,
        String workerState,
        int threadNum,
        String error
) {}
