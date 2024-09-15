package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.metric.Counter;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.transfer.Handover;
import com.bilik.ditto.implementations.s3.S3Downloader.FileS3Downloader;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3ConsumerThreadTest {

    @Test
    void testRun_withoutInitialization() {
        // given
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);
        Handover<PathWrapper> handover = mock(Handover.class);
        S3Split split = mock(S3Split.class);
        CounterAggregator counterAggregator = new CounterAggregator();

        S3ConsumerThread consumer = new S3ConsumerThread(
                eventProducer,
                fileDownloader,
                handover,
                split,
                counterAggregator
        );

        // when
        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(consumer::run)
                .withMessage("WorkerThread has not been initialized yet!");
    }

    @Test
    void testRun_emptySplit_shouldEndFine() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);
        Handover<PathWrapper> handover = mock(Handover.class);
        S3Split split = mock(S3Split.class);
        CounterAggregator counterAggregator = new CounterAggregator();

        List<String> objects = Collections.emptyList();
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString())).thenAnswer(
                mock -> Path.of("/test"));


        S3ConsumerThread consumer = new S3ConsumerThread(
                eventProducer,
                fileDownloader,
                handover,
                split,
                counterAggregator
        );
        // when
        consumer.initThread();
        consumer.run();

        // then
        verify(fileDownloader, times(0)).download(anyString());

        verify(handover, times(0)).produce(any(PathWrapper.class));
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));

        Counter counter = counterAggregator.getCounters().values().iterator().next();
        assertThat(counter.getCount()).isZero();

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
    }

    @Test
    void test_singleObject() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        String threadName = "name-1";
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);
        Handover<PathWrapper> handover = mock(Handover.class);
        S3Split split = mock(S3Split.class);
        CounterAggregator counterAggregator = new CounterAggregator();

        List<String> objects = List.of("testObj");
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString())).thenAnswer(
                mock -> Path.of("/test"));


        S3ConsumerThread consumer = new S3ConsumerThread(
                eventProducer,
                fileDownloader,
                handover,
                split,
                counterAggregator
        );
        consumer.setName(threadName);
        // when
        consumer.initThread();
        consumer.run();

        // then
        verify(fileDownloader, times(1)).download(anyString());

        verify(handover, times(1)).produce(any(PathWrapper.class));
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));

        assertThat(counterAggregator.getCounters().size()).isOne();
        assertThat(counterAggregator.getCounters().get(threadName + "_read_objects").getCount()).isOne();

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
    }

    @Test
    void test_multipleObjects() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        String threadName = "name-1";
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);
        Handover<PathWrapper> handover = mock(Handover.class);
        S3Split split = mock(S3Split.class);
        CounterAggregator counterAggregator = new CounterAggregator();

        List<String> objects = List.of("testObj", "testObj1", "testObj2");
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString())).thenAnswer(
                mock -> Path.of("/test"));


        S3ConsumerThread consumer = new S3ConsumerThread(
                eventProducer,
                fileDownloader,
                handover,
                split,
                counterAggregator
        );
        consumer.setName(threadName);
        // when
        consumer.initThread();
        consumer.run();

        // then
        verify(fileDownloader, times(objects.size())).download(anyString());

        verify(handover, times(objects.size())).produce(any(PathWrapper.class));
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));

        assertThat(counterAggregator.getCounters().size()).isOne();
        assertThat(counterAggregator.getCounters().get(threadName + "_read_objects").getCount())
                .isEqualTo(objects.size());

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
    }

}
