package com.bilik.ditto.api.domain.converter;

import com.bilik.ditto.api.domain.dao.entity.CounterDao;
import com.bilik.ditto.api.domain.dao.entity.JobDao;
import com.bilik.ditto.api.domain.dao.entity.WorkerEventDao;
import com.bilik.ditto.api.domain.dto.JobDto;

import java.util.stream.Collectors;

public class JobDaoToDto {

    public static JobDto daoToDto(JobDao dao) {
        return new JobDto(
                dao.getId(),
                dao.getJobId(),
                dao.getParallelism(),
                dao.getError(),
                dao.getQueueSize(),
                dao.isQueued(),
                dao.getWorkerEvents().stream()
                        .map(JobDaoToDto::eventToDto)
                        .collect(Collectors.toSet()),
                dao.getSourceType(),
                dao.getSinkType(),
                dao.getSourceDataType(),
                dao.getSinkDataType(),
                dao.getSpecialConverterName(),
                dao.getSpecialConverterSubtype(),
                dao.getSpecialConverterArgs(),
                dao.getCreationTime(),
                dao.getStartTime(),
                dao.getRunningTime(),
                dao.getFinishTime(),
                dao.getCounters().stream()
                        .map(JobDaoToDto::counterToDto)
                        .collect(Collectors.toSet())
        );
    }

    public static JobDto.CounterDto counterToDto(CounterDao dao) {
        return new JobDto.CounterDto(
                dao.getId(),
                dao.getName(),
                dao.getCounterVal()
        );
    }

    public static JobDto.WorkerEventDto eventToDto(WorkerEventDao dao) {
        return new JobDto.WorkerEventDto(
          dao.getId(),
          dao.getWorkerType(),
          dao.getWorkerState(),
          dao.getThreadNum()
        );
    }
}
