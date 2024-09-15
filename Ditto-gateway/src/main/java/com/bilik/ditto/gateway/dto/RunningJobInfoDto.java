package com.bilik.ditto.gateway.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record RunningJobInfoDto(String jobId,
                                int parallelism,
                                List<WorkerEventDto> workerEvents,
                                boolean isFinished,
                                Map<Integer, Integer> sourceQueueSizes,
                                Map<Integer, Integer> sinkQueueSizes,
                                Map<String, Long> counters,
                                Instant creationTime,
                                Instant startTime,
                                Instant runningTime) {
}
