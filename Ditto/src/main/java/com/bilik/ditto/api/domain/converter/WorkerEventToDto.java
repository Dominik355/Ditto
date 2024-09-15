package com.bilik.ditto.api.domain.converter;

import com.bilik.ditto.api.domain.dto.WorkerEventDto;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class WorkerEventToDto {

    public static List<WorkerEventDto> from(Collection<WorkerEvent> workerEvents) {
        var ret = new ArrayList<WorkerEventDto>();
        for (var event : workerEvents) {
            ret.add(new WorkerEventDto(
                    event.getType().name,
                    event.getWorkerState().name(),
                    event.getThreadNum(),
                    event.getError()
            ));
        }
        return ret;
    }

    public static WorkerEventDto from(WorkerEvent workerEvent) {
        return new WorkerEventDto(
                workerEvent.getType().name,
                workerEvent.getWorkerState().name(),
                workerEvent.getThreadNum(),
                workerEvent.getError()
        );
    }

}
