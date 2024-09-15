package com.bilik.ditto.core.job.output;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.job.output.accumulation.DummyAccumulator;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import com.bilik.ditto.testCommons.SensorGenerator;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

public class SinkTest {

    static final String JOB_ID = "test-job-0";
    static final int PARALLELISM = 3;
    static final int ACCUMULATOR_SIZE = 3;

    private Sink<Sensor> sink;
    private List<DummyAccumulator<Sensor>> accumulators;
    private Map<Integer, BlockingQueue<StreamElement<Sensor>>> queues;

    @BeforeEach
    void init() {
        accumulators = new ArrayList<>(PARALLELISM);
        for (int i = 1; i <= PARALLELISM; i++) {
            accumulators.add(new DummyAccumulator<>(ACCUMULATOR_SIZE));
        }

        sink = new Sink<>(
                new ProtobufType<>(Sensor.class),
                JOB_ID,
                new MockingWorkerEventProducer(),
                mock(ExecutorService.class),
                i -> accumulators.get(i - 1)
        );
    }

    @Test
    void initializeTest_emptyWorkerMap() {
        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() ->  sink.initialize(Collections.emptyMap()));
    }

    @Test
    void initializeTest() {
        initializeSink();
        assertThat(sink.getParallelism()).isEqualTo(PARALLELISM);
    }

    @Test
    void workerFinishedTest_notInitialized() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() ->  sink.workerFinished(1));
    }

    @Test
    void workerFinishedTest_unknownWorkerId() {
        initializeSink();
        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() ->  sink.workerFinished(321));
    }

    @Test
    void workerFinishedTest_repeatedWorkerId() {
        initializeSink();

        sink.workerFinished(1);
        sink.workerFinished(1);

        assertThat(sink.finishedWorkers()).containsExactly(1);
    }

    @Test
    void workerFinishedTest_allWorkers_onfinishNotSet() {
        initializeSink();

        sink.workerFinished(1);
        sink.workerFinished(2);
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> sink.workerFinished(3));
    }

    @Test
    void workerFinishedTest_allWorkers() {
        initializeSink();
        AtomicBoolean flag = new AtomicBoolean();
        sink.setOnFinish(() -> flag.set(true));

        for (int i = 1; i <= PARALLELISM; i++) {
            sink.workerFinished(i);
        }

        assertThat(flag.get()).isTrue();
    }

    @Test
    void start_emptyWorkers() {
        assertThat(sink.start()).isFalse();
    }

    @Test
    void startStopTest() throws Exception {
        initializeSink();

        assertThat(sink.start()).isTrue();

        Set<Thread> startedWorkers = Thread.getAllStackTraces().keySet().stream()
                        .filter(thread -> thread instanceof WorkerThread)
                        .collect(Collectors.toSet());

        assertThat(startedWorkers).hasSize(PARALLELISM);
        for(Thread t : startedWorkers) {
            assertThat(t.getName()).contains(JOB_ID);
        }

        // after
        sink.stop();
    }

    @Test
    void accumulatorProviderTest_dependsOnWorkingSinkWorker() throws Exception {
        initializeSink();
        sink.start();

        for (int i = 0; i < ACCUMULATOR_SIZE; i++) {
            for (BlockingQueue<StreamElement<Sensor>> queue : queues.values()) {
                queue.offer(StreamElement.of(SensorGenerator.getSingle()));
            }
        }

        sink.stop();

        // then
        Set<Thread> activeWorkers = Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread instanceof WorkerThread)
                .collect(Collectors.toSet());

        for (Thread t : activeWorkers){
            t.join(5_000);
            assertThat(t.isAlive()).isFalse();
        }

        for (DummyAccumulator<Sensor> accumulator : accumulators) {
            assertThat(accumulator.getFullCounter()).isOne();
        }
    }

    private void initializeSink() {
        queues = new HashMap<>();
        for (int i = 1; i <= PARALLELISM; i++) {
            queues.put(i, new LinkedBlockingQueue<>());
        }

        sink.initialize(queues);
    }
}
