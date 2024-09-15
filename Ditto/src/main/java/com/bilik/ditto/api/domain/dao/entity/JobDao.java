package com.bilik.ditto.api.domain.dao.entity;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.metric.Counter;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
@Table("job")
@NoArgsConstructor
public class JobDao {

    @Id
    private long id;
    private String jobId;
    private int parallelism;
    private String error;
    private int queueSize;
    private boolean queued;

    @Setter(AccessLevel.PRIVATE)
    private Set<WorkerEventDao> workerEvents = new HashSet<>();
    private String sourceType;
    private String sinkType;
    private String sourceDataType;
    private String sinkDataType;
    private String specialConverterName;
    private String specialConverterSubtype;
    private String specialConverterArgs;

    private LocalDateTime creationTime;
    private LocalDateTime startTime;
    private LocalDateTime runningTime;
    private LocalDateTime finishTime;

    @Setter(AccessLevel.PRIVATE)
    private Set<CounterDao> counters = new HashSet<>();

    public void addWorkerEvent(WorkerEvent event) {
        workerEvents.add(WorkerEventDao.fromWorkerEvent(event));
    }

    public void addCounter(Map.Entry<String, Counter> entry) {
        counters.add(new CounterDao(entry.getKey(), entry.getValue().getCount()));
    }

}
