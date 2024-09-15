package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.metric.Counter;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import com.bilik.ditto.core.transfer.Handover;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.*;

public class HdfsConsumerThreadTest {

    @Test
    void testRun_withoutInitialization() {
        // given
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);
        Handover<PathWrapper> handover = mock(Handover.class);
        HdfsSplit split = mock(HdfsSplit.class);
        CounterAggregator counterAggregator = new CounterAggregator();

        HdfsConsumerThread consumer = new HdfsConsumerThread(
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
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);
        Handover<PathWrapper> handover = mock(Handover.class);
        HdfsSplit split = mock(HdfsSplit.class);
        CounterAggregator counterAggregator = new CounterAggregator();

        when(split.iterator()).thenAnswer(mock -> Collections.emptyIterator());
        when(split.size()).thenAnswer(mock -> 0);
        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> java.nio.file.Path.of(((Path) mock.getArguments()[0]).toUri().toString()));


        HdfsConsumerThread consumer = new HdfsConsumerThread(
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
        verify(fileDownloader, times(0)).download(ArgumentMatchers.any(Path.class));

        verify(handover, times(0)).produce(any(PathWrapper.class));
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));

        Counter counter = counterAggregator.getCounters().values().iterator().next();
        assertThat(counter.getCount()).isZero();

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
    }

    @Test
    void test_singlePath() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);
        Handover<PathWrapper> handover = mock(Handover.class);
        HdfsSplit split = mock(HdfsSplit.class);
        CounterAggregator counterAggregator = new CounterAggregator();

        when(split.iterator()).thenAnswer(mock -> List.of(new Path("test")).iterator());
        when(split.size()).thenAnswer(mock -> 1);
        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> java.nio.file.Path.of(((Path) mock.getArguments()[0]).toUri().toString()));


        HdfsConsumerThread consumer = new HdfsConsumerThread(
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
        verify(fileDownloader, times(1)).download(ArgumentMatchers.any(Path.class));

        ArgumentCaptor<PathWrapper> handoverCaptor = ArgumentCaptor.forClass(PathWrapper.class);
        verify(handover, times(1)).produce(handoverCaptor.capture());
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));
        assertThat(handoverCaptor.getValue()).isEqualTo(new PathWrapper(java.nio.file.Path.of("test")));

        Counter counter = counterAggregator.getCounters().values().iterator().next();
        assertThat(counter.getCount()).isOne();

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
    }

    @Test
    void test_multiplePaths() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);
        Handover<PathWrapper> handover = mock(Handover.class);
        HdfsSplit split = mock(HdfsSplit.class);
        CounterAggregator counterAggregator = new CounterAggregator();
        List<Path> paths = IntStream.range(0, 10)
                .mapToObj(i -> new Path("/test" + i))
                .toList();

        when(split.iterator()).thenAnswer(mock -> paths.iterator());
        when(split.size()).thenAnswer(mock -> paths.size());
        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> java.nio.file.Path.of(((Path) mock.getArguments()[0]).toUri().toString()));

        HdfsConsumerThread consumer = new HdfsConsumerThread(
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
        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);

        verify(fileDownloader, times(paths.size())).download(ArgumentMatchers.any(Path.class));

        ArgumentCaptor<PathWrapper> handoverCaptor = ArgumentCaptor.forClass(PathWrapper.class);
        verify(handover, times(paths.size())).produce(handoverCaptor.capture());
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));

        Counter counter = counterAggregator.getCounters().values().iterator().next();
        assertThat(counter.getCount()).isEqualTo(10);

        assertThat(handoverCaptor.getAllValues().size()).isEqualTo(paths.size());
        Iterator<Path> pathIterator = paths.iterator();
        Iterator<PathWrapper> captorIterator = handoverCaptor.getAllValues().iterator();
        while (pathIterator.hasNext()) {
            Path p1 = pathIterator.next();
            PathWrapper p2 = captorIterator.next();
            assertThat(p1.getName()).isEqualTo(p2.getHadoopPath().getName());
        }
    }
}
