package com.bilik.ditto.core.job.input;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import proto.test.Sensor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SourceTest {

    static final String JOB_ID = "test-job-0";
    static final int PARALLELISM = 3;

    private DummySource source;

    @BeforeEach
    void init() {
        source = new DummySource(
                PARALLELISM,
                new ProtobufType<>(Sensor.class),
                JOB_ID,
                new MockingWorkerEventProducer());
    }

    @Test
    void workerFinishedTest_notInitialized() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() ->  source.workerFinished(1));
    }

    @Test
    void workerFinishedTest_unknownWorkerId() {
        source.initialize(null);

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() ->  source.workerFinished(321));
    }

    @Test
    void workerFinishedTest_repeatedWorkerId() {
        source.initialize(null);

        source.workerFinished(1);
        source.workerFinished(1);

        assertThat(source.finishedWorkers()).containsExactly(1);
    }

    @Test
    void workerFinishedTest_allWorkers_onfinishNotSet() {
        source.initialize(null);

        source.workerFinished(1);
        source.workerFinished(2);
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> source.workerFinished(3));
    }

    @Test
    void workerFinishedTest_allWorkers() {
        source.initialize(null);

        AtomicBoolean flag = new AtomicBoolean();
        source.setOnFinish(() -> flag.set(true));

        for (int i = 1; i <= PARALLELISM; i++) {
            source.workerFinished(i);
        }

        assertThat(flag.get()).isTrue();
    }

    @Test
    void start_emptyWorkers() {
        assertThat(source.start()).isFalse();
    }

    @Test
    void startStopTest() throws Exception {
        source.initialize(null);

        assertThat(source.start()).isTrue();

        Set<Thread> startedWorkers = Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread instanceof WorkerThread)
                .collect(Collectors.toSet());

        source.assertWorkersStarted();

        // after
        source.stop();
    }

    static class DummySource<T> extends Source<T> {

        private boolean initialized;

        protected DummySource(int parallelism, Type<T> type, String jobId, WorkerEventProducer eventProducer) {
            super(parallelism, type, jobId, eventProducer);
        }

        @Override
        public Collection<Integer> initialize(Map<Integer, BlockingQueue<StreamElement<T>>> workerMap) {
            initialized = true;

            workerThreads = new HashMap<>();
            for (int i = 1; i <= parallelism; i++) {
                workerThreads.put(i, Mockito.mock(WorkerThread.class));
            }

            return workerThreads.keySet();
        }

        public boolean isInitialized() {
            return initialized;
        }

        public void assertWorkersStarted() {
            workerThreads.values().forEach(t -> verify(t, times(1)).start());
        }
    }
    
}
