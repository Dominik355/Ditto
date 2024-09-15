package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.transfer.Handover;
import com.bilik.ditto.core.util.FileUtils;
import com.bilik.ditto.implementations.s3.S3Downloader.FileS3Downloader;
import com.bilik.ditto.testCommons.DummyFileReader;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import com.bilik.ditto.testCommons.ThreadUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3WorkerTest {

    @Test
    void run_notInitialized() throws Exception {
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        S3Split split = mock(S3Split.class);
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);

        List<String> objects = IntStream.range(0, 1)
                .mapToObj(i -> "test-" + i)
                .toList();
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString())).thenAnswer(
                mock -> Path.of("/test"));
        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        S3Worker<byte[]> worker = new S3Worker<>(
                workerEventProducer,
                fileRWFactory,
                queue,
                counterAggregator,
                split,
                fileDownloader
        );

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(worker::run)
                .withMessage("WorkerThread has not been initialized yet!");
    }

    @Test
    void initializationTest() throws Exception {
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        S3Split split = mock(S3Split.class);
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);

        List<String> objects = IntStream.range(0, 1)
                .mapToObj(i -> "test-" + i)
                .toList();
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString())).thenAnswer(
                mock -> Path.of("/test"));
        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        S3Worker<byte[]> worker = new S3Worker<>(
                workerEventProducer,
                fileRWFactory,
                queue,
                counterAggregator,
                split,
                fileDownloader
        );

        assertThat(worker.initThread()).isTrue();
        assertThat(counterAggregator.getCounters()).hasSize(3);
    }

    @Test
    void test_emptyIterator() throws Exception {
        // given
        String threadName = "S3-Worker-test";
        MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class);
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        S3Split split = mock(S3Split.class);
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);

        List<String> objects = Collections.emptyList();
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString())).thenAnswer(
                mock -> Path.of("/test"));
        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        S3Worker<byte[]> worker = new S3Worker<>(
                workerEventProducer,
                fileRWFactory,
                queue,
                counterAggregator,
                split,
                fileDownloader
        );
        worker.setName(threadName);

        // when
        worker.initThread();
        worker.run();
        worker.shutdown();

        // then
        fileUtilsMock.verify(() -> FileUtils.deleteFile(any(java.nio.file.Path.class)), times(0));

        verify(fileRWFactory, times(0)).buildFileReader(any(PathWrapper.class));

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_objects").getCount())
                .isEqualTo(0);
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(0);

        assertThat(queue.size()).isEqualTo(0);
        ThreadUtils.assertNoWorkerThreadsAlive();

        // after
        fileUtilsMock.close();
    }

    @Test
    void test_handoverClosedByConsumerOnFirstDownload() throws Exception {
        // given
        String threadName = "S3-Worker-test";
        MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class);
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        S3Split split = mock(S3Split.class);
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);

        List<String> objects = IntStream.range(0, 5)
                .mapToObj(i -> "test-" + i)
                .toList();
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString())).thenAnswer(
                mock -> {throw new Handover.ClosedException();});
        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        S3Worker<byte[]> worker = new S3Worker<>(
                workerEventProducer,
                fileRWFactory,
                queue,
                counterAggregator,
                split,
                fileDownloader
        );
        worker.setName(threadName);

        // when
        worker.initThread();
        worker.run();
        worker.shutdown();

        // then
        fileUtilsMock.verify(() -> FileUtils.deleteFile(any(Path.class)), times(0));

        verify(fileRWFactory, times(0)).buildFileReader(any(PathWrapper.class));

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_objects").getCount())
                .isEqualTo(0);
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(0);

        assertThat(queue.size()).isEqualTo(0);
        ThreadUtils.assertNoWorkerThreadsAlive();

        // after
        fileUtilsMock.close();
    }

    @Test
    void test_exceptionInConsumerThread() throws Exception {
        // given
        String threadName = "S3-Worker-test";
        MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class);
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        S3Split split = mock(S3Split.class);
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);

        List<String> objects = IntStream.range(0, 5)
                .mapToObj(i -> "test-" + i)
                .toList();
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString()))
                .thenAnswer(mock -> {throw new RuntimeException();});
        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        S3Worker<byte[]> worker = new S3Worker<>(
                workerEventProducer,
                fileRWFactory,
                queue,
                counterAggregator,
                split,
                fileDownloader
        );
        worker.setName(threadName);

        // when
        worker.initThread();
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(worker::run);
        worker.shutdown();

        // then
        fileUtilsMock.verify(() -> FileUtils.deleteFile(any(java.nio.file.Path.class)), times(0));

        verify(fileRWFactory, times(0)).buildFileReader(any(PathWrapper.class));

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_objects").getCount())
                .isEqualTo(0);
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(0);

        assertThat(queue.size()).isEqualTo(0);
        ThreadUtils.assertNoWorkerThreadsAlive();

        // after
        fileUtilsMock.close();
    }


    @ParameterizedTest
    @ValueSource(ints = {1, 5, 33})
    void test_allRight(int range) throws Exception {
        // given
        String threadName = "S3-Worker-test";
        int elementsPerFile = 5;
        MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class);
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        S3Split split = mock(S3Split.class);
        FileS3Downloader fileDownloader = mock(FileS3Downloader.class);

        List<String> objects = IntStream.range(0, range)
                .mapToObj(i -> "test-" + i)
                .toList();
        when(split.iterator()).thenAnswer(mock -> objects.iterator());
        when(split.size()).thenAnswer(mock -> objects.size());
        when(fileDownloader.download(anyString())).thenAnswer(
                mock -> Path.of("/test"));
        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(elementsPerFile));

        S3Worker<byte[]> worker = new S3Worker<>(
                workerEventProducer,
                fileRWFactory,
                queue,
                counterAggregator,
                split,
                fileDownloader
        );
        worker.setName(threadName);

        // when
        worker.initThread();
        worker.run();
        worker.shutdown();

        // then
        fileUtilsMock.verify(() -> FileUtils.deleteFile(any(java.nio.file.Path.class)), times(objects.size()));

        verify(fileRWFactory, times(objects.size())).buildFileReader(any(PathWrapper.class));

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_objects").getCount())
                .isEqualTo(objects.size());
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(elementsPerFile * objects.size());

        assertThat(queue.size()).isEqualTo(elementsPerFile * objects.size());
        StreamElement<byte[]> element;
        while ((element = queue.poll()) != null) {
            assertThat(element.value()).isNotNull();
        }

        ThreadUtils.assertNoWorkerThreadsAlive();

        // after
        fileUtilsMock.close();
    }

}
