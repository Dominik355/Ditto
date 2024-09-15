package com.bilik.ditto.api.domain.converter;

import com.bilik.ditto.api.domain.dto.RunningJobInfoDto;
import com.bilik.ditto.core.job.JobOrchestration;

import java.util.Map;
import java.util.stream.Collectors;

public class JobOrchstrationToRunningJobInfoDto {

    public static RunningJobInfoDto from(JobOrchestration<?, ?> jobOrchestration) {
        Map<String, Long> counters = jobOrchestration.getSourceCounterAggregator()
                .getCounters()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getCount()
                ));

        jobOrchestration.getSinkCounterAggregator()
                .getCounters()
                .entrySet()
                .forEach(entry -> counters.put(
                        entry.getKey(),
                        entry.getValue().getCount()));

        return new RunningJobInfoDto(
                jobOrchestration.getJobId(),
                jobOrchestration.getParallelism(),
                WorkerEventToDto.from(jobOrchestration.getWorkerEventsView()),
                jobOrchestration.isFinished(),
                jobOrchestration.getSourceQueueSizes(),
                jobOrchestration.getSinkQueueSizes(),
                counters,
                jobOrchestration.getCreationTime(),
                jobOrchestration.getStartTime(),
                jobOrchestration.getRunningTime()
        );
    }

}
