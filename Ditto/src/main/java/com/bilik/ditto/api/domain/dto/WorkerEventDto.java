package com.bilik.ditto.api.domain.dto;

public record WorkerEventDto(
        String workerType,
        String workerState,
        int threadNum,
        String error
) {}
