package com.bilik.ditto.core.job;

import com.bilik.ditto.core.concurrent.threadCommunication.QueueWorkerEventCommunicator;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class MonotonousJobOrchestrationTest {

    static final int PARALLELISM = 5;
    static final String JOB_ID = "test-job-0";

    private Source<Sensor> source;
    private Sink<Sensor> sink;
    private MonotonousJobOrchestration<Sensor> jobOrchestration;

    @BeforeEach
    void init() {
        source = mock(Source.class);
        sink = mock(Sink.class);

        jobOrchestration = new MonotonousJobOrchestration(
                source,
                sink,
                null,
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                null
        );
    }

    @Test
    void initializeQueuesTest() {
        jobOrchestration.initializeQueues();

        assertThat(jobOrchestration.sourceQueues).hasSize(PARALLELISM);
        assertThat(jobOrchestration.sinkQueues).hasSize(PARALLELISM);

        for(Map.Entry<Integer, BlockingQueue<StreamElement<Sensor>>> sourceEntry : jobOrchestration.sourceQueues.entrySet()) {
            assertThat(sourceEntry.getValue()).isEqualTo(jobOrchestration.sinkQueues.get(sourceEntry.getKey()));
        }
    }

    @Test
    void initializeConvertersTest() {
        jobOrchestration.initializeConverters();

        assertThat(jobOrchestration.converterWorkers).isNull();
    }

}
