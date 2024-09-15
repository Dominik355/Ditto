package com.bilik.ditto.core.job;

import com.bilik.ditto.core.concurrent.WorkerThread.WorkerType;
import com.bilik.ditto.core.concurrent.threadCommunication.QueueWorkerEventCommunicator;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent.WorkerState;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventCommunicator;
import com.bilik.ditto.core.configuration.DittoConfiguration.OrchestrationConfig;
import com.bilik.ditto.core.convertion.ConverterWorker;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import com.bilik.ditto.testCommons.ThreadUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import proto.test.Sensor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.bilik.ditto.core.job.JobOrchestration.DEFAULT_QUEUE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobOrchestrationTest {

    static final int PARALLELISM = 5;
    static final String JOB_ID = "test-job-0";

    private Source<Sensor> source;
    private Sink<Sensor> sink;
    private DummyJobOrchestration jobOrchestration;

    @BeforeEach
    void init() {
        source = mock(Source.class);
        sink = mock(Sink.class);

        jobOrchestration = new DummyJobOrchestration(
                source,
                sink,
                null,
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                null,
                true
        );
    }

    @Test
    void handleEvent_source_finished() {
        jobOrchestration.handleEvent(new WorkerEvent(WorkerState.FINISHED, WorkerType.SOURCE));
        verify(source, times(1)).workerFinished(anyInt());
    }

    @Test
    void handleEvent_sink_finished() {
        jobOrchestration.handleEvent(new WorkerEvent(WorkerState.FINISHED, WorkerType.SINK));
        verify(sink, times(1)).workerFinished(anyInt());
    }

    @ParameterizedTest
    @EnumSource(WorkerType.class)
    void handleEvent_error(WorkerType workerType) {
        jobOrchestration.handleEvent(new WorkerEvent(WorkerState.ERROR, workerType));
        assertThat(jobOrchestration.hasBeenStopped).isTrue();
    }

    @Test
    void initializeOrchestration_default() {
        when(source.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());
        when(sink.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());

        jobOrchestration.initializeOrchestration();
        assertThat(jobOrchestration.parallelism).isEqualTo(PARALLELISM);
        assertThat(jobOrchestration.sourceQueues).containsKeys(1, 2, 3, 4, 5);
        assertThat(jobOrchestration.sinkQueues).containsKeys(1, 2, 3, 4, 5);
        assertThat(jobOrchestration.orchestrationEventLoop).isNotNull();
        assertThat(jobOrchestration.initializedQueus).isTrue();
        assertThat(jobOrchestration.initializedConverters).isTrue();

        verify(source, times(1)).initialize(anyMap());
        verify(sink, times(1)).initialize(anyMap());

        verify(source, times(1)).setOnFinish(any(Runnable.class));
        verify(sink, times(1)).setOnFinish(any(Runnable.class));
    }

    @Test
    void initializeOrchestration_lessSourcesThanParallelism() {
        when(source.initialize(anyMap())).thenAnswer(i -> {
            Set<Integer> keys = ((Map<Integer, Object>) i.getArguments()[0]).keySet();
            keys.remove(5);
            return keys;
        });
        when(sink.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());

        jobOrchestration.initializeOrchestration();
        assertThat(jobOrchestration.parallelism).isEqualTo(PARALLELISM - 1);
        assertThat(jobOrchestration.sourceQueues).containsKeys(1, 2, 3, 4);
        assertThat(jobOrchestration.sinkQueues).containsKeys(1, 2, 3, 4);
        assertThat(jobOrchestration.orchestrationEventLoop).isNotNull();
        assertThat(jobOrchestration.initializedQueus).isTrue();
        assertThat(jobOrchestration.initializedConverters).isTrue();

        verify(source, times(1)).initialize(anyMap());
        verify(sink, times(1)).initialize(anyMap());

        verify(source, times(1)).setOnFinish(any(Runnable.class));
        verify(sink, times(1)).setOnFinish(any(Runnable.class));
    }

    @Test
    void initializeOrchestration_sourceInitializeZero() {
        when(source.initialize(anyMap())).thenAnswer(i -> Collections.emptyList());
        when(sink.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(jobOrchestration::initializeOrchestration);
    }

    @Test
    void sourceFinished_notInitializedOrchestration() {
        assertThatNullPointerException().isThrownBy(jobOrchestration::sourceFinished);
    }

    @Test
    void sourceFinished_test_1() {
        when(source.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());
        when(sink.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());

        jobOrchestration.initializeOrchestration();
        jobOrchestration.sourceFinished();

        for(Map.Entry<Integer, BlockingQueue<StreamElement<Sensor>>> entry : jobOrchestration.sourceQueues.entrySet()) {
            assertThat(entry.getValue().poll()).isEqualTo(StreamElement.last());
        }
    }

    @Test
    void sinkFinished_test_1() {
        jobOrchestration.sinkFinished();
        assertThat(jobOrchestration.hasBeenStopped).isTrue();
    }

    @Test
    void getQueueSize_noConfiguratedSize() {
        assertThat(jobOrchestration.getQueueSize(false)).isEqualTo(DEFAULT_QUEUE_SIZE / PARALLELISM);
    }

    @Test
    void getQueueSize_configuredSize_nonConverting() {
        int size = 100;
        jobOrchestration = new DummyJobOrchestration(
                source,
                sink,
                new OrchestrationConfig(size),
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                null,
                false
        );
        assertThat(jobOrchestration.getQueueSize(false)).isEqualTo(size / PARALLELISM);
    }

    @Test
    void getQueueSize_configuredSize_converting() {
        int size = 100;
        jobOrchestration = new DummyJobOrchestration(
                source,
                sink,
                new OrchestrationConfig(size),
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                null,
                true
        );
        assertThat(jobOrchestration.getQueueSize(true)).isEqualTo(size / PARALLELISM / 2);
    }

    @Test
    void getQueueSize_parallelismSmallerThanQueueSize_nonConverting() {
        int size = 2;
        jobOrchestration = new DummyJobOrchestration(
                source,
                sink,
                new OrchestrationConfig(size),
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                null,
                false
        );
        assertThat(jobOrchestration.getQueueSize(false)).isEqualTo(size);
    }

    @Test
    void getQueueSize_parallelismSmallerThanQueueSize_converting() {
        int size = 2;
        jobOrchestration = new DummyJobOrchestration(
                source,
                sink,
                new OrchestrationConfig(size),
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                null,
                true
        );
        assertThat(jobOrchestration.getQueueSize(true)).isEqualTo(size);
    }

    @Test
    void stopTest_monotonous() throws Exception {
        // given
        when(source.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());
        when(sink.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());

        AtomicBoolean whenFinishedCalled = new AtomicBoolean(false);
        Consumer<JobOrchestration<?,?>> whenFinished = jobId -> whenFinishedCalled.set(true);
        JobOrchestration<Sensor, Sensor> jobOrchestration = new MonotonousJobOrchestration<>(
                source,
                sink,
                null,
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                whenFinished
        );

        // when
        jobOrchestration.initializeOrchestration();
        jobOrchestration.run();
        jobOrchestration.stop();

        // then
        verify(source, times(1)).stop();
        verify(sink, times(1)).stop();

        assertThat(jobOrchestration.isFinished()).isTrue();
        assertThat(whenFinishedCalled.get()).isTrue();
        ThreadUtils.assertThreadTermination(jobOrchestration.orchestrationEventLoop);
    }

    @Test
    void stopTest_converting() throws Exception {
        // given
        when(source.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());
        when(sink.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());

        AtomicBoolean whenFinishedCalled = new AtomicBoolean(false);
        Consumer<JobOrchestration<?,?>> whenFinished = jobId -> whenFinishedCalled.set(true);
        JobOrchestration<Sensor, Sensor> jobOrchestration = new ConvertingJobOrchestration<>(
                source,
                sink,
                null,
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                whenFinished,
                () -> null
        );

        // when
        jobOrchestration.initializeOrchestration();
        jobOrchestration.run();
        jobOrchestration.stop();

        // then
        verify(source, times(1)).stop();
        verify(sink, times(1)).stop();

        for (Map.Entry<Integer, ConverterWorker<Sensor, Sensor>> entry : jobOrchestration.converterWorkers.entrySet()) {
            ThreadUtils.assertThreadTermination(entry.getValue());
        }

        assertThat(jobOrchestration.isFinished()).isTrue();
        assertThat(whenFinishedCalled.get()).isTrue();
        ThreadUtils.assertThreadTermination(jobOrchestration.orchestrationEventLoop);
    }

    @Test
    void runTest() throws Exception {
        // given
        when(source.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());
        when(sink.initialize(anyMap())).thenAnswer(i -> ((Map<Integer, Object>) i.getArguments()[0]).keySet());

        JobOrchestration<Sensor, Sensor> jobOrchestration = new ConvertingJobOrchestration<>(
                source,
                sink,
                null,
                new QueueWorkerEventCommunicator(),
                JOB_ID,
                PARALLELISM,
                null,
                () -> null
        );

        // when
        jobOrchestration.initializeOrchestration();
        jobOrchestration.run();

        // then
        assertThat(jobOrchestration.orchestrationEventLoop.isAlive()).isTrue();
        verify(source, times(1)).start();
        verify(sink, times(1)).start();

        for (Map.Entry<Integer, ConverterWorker<Sensor, Sensor>> entry : jobOrchestration.converterWorkers.entrySet()) {
            assertThat(entry.getValue().isAlive()).isTrue();
        }

        // dont forget to stop everything
        jobOrchestration.stop();
    }

    @Test
    void getJobIdTest() {
        assertThat(jobOrchestration.getJobId()).isEqualTo(JOB_ID);
    }

    static class DummyJobOrchestration extends JobOrchestration<Sensor, Sensor> {

        boolean hasBeenStopped;
        boolean initializedQueus, initializedConverters;
        boolean isConverting;

        DummyJobOrchestration(Source<Sensor> source,
                              Sink<Sensor> sink,
                              OrchestrationConfig configuration,
                              WorkerEventCommunicator workerEventCommunicator,
                              String jobId,
                              int parallelism,
                              Consumer<JobOrchestration<?,?>> whenFinished,
                              boolean isConverting) {
            super(source, sink, configuration, workerEventCommunicator, jobId, parallelism, whenFinished);
            this.isConverting = isConverting;
        }

        @Override
        protected void initializeQueues() {
            initializedQueus = true;
            this.sourceQueues = IntStream.rangeClosed(1, parallelism)
                    .boxed()
                    .collect(Collectors.toMap(
                            Function.identity(),
                            i -> new LinkedBlockingQueue<>(getQueueSize(isConverting))
                    ));

            this.sinkQueues = new HashMap<>();
            sinkQueues.putAll(sourceQueues);
        }

        @Override
        protected void initializeConverters() {
            initializedConverters = true;
        }

        @Override
        public void stop() throws Exception {
            hasBeenStopped = true;
        }
    }

}
