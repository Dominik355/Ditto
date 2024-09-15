package com.bilik.ditto.gateway.dto;

import java.time.LocalDateTime;
import java.util.Set;

public record JobDto(long id, 
                     String jobId,
                     int parallelism,
                     String error,
                     int queueSize,
                     boolean queued,
                     Set<WorkerEventDto> workerEvents,
                     String sourceType,
                     String sinkType,
                     String sourceDataType,
                     String sinkDataType,
                     String specialConverterName,
                     String specialConverterSubtype,
                     String specialConverterArgs,
                     LocalDateTime creationTime,
                     LocalDateTime startTime,
                     LocalDateTime runningTime,
                     LocalDateTime finishTime,
                     Set<CounterDto> counters) {

    public record CounterDto(long id,
                             String name,
                             Long value) {}

    public record WorkerEventDto(long id,
                                 String workerType,
                                 String workerState,
                                 int threadNum) {}


}
