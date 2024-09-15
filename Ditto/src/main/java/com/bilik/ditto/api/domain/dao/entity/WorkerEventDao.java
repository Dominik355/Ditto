package com.bilik.ditto.api.domain.dao.entity;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;

@Data
@Table("worker_event")
public class WorkerEventDao {

    @Id
    private long id;
    @Transient
    private JobDao job;
    private String workerType;
    private String workerState;
    private int threadNum;

    public WorkerEventDao(String workerType, String workerState, int threadNum) {
        this.workerType = workerType;
        this.workerState = workerState;
        this.threadNum = threadNum;
    }

    public static WorkerEventDao fromWorkerEvent(WorkerEvent event) {
        return new WorkerEventDao(
                event.getType().name,
                event.getWorkerState().name(),
                event.getThreadNum()
        );
    }
}
