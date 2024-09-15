package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.transfer.Handover;
import com.bilik.ditto.core.util.FileUtils;
import com.bilik.ditto.testCommons.DummyFileReader;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import com.bilik.ditto.testCommons.ThreadUtils;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HdfsWorkerTest {

    @Test
    void run_notInitialized() throws Exception {
        List<Path> paths = IntStream.range(0, 1)
                .mapToObj(i -> new Path("/test" + i))
                .toList();

        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        HdfsSplit split = mock(HdfsSplit.class);
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);

        when(split.iterator()).thenAnswer(mock -> paths.iterator());

        when(split.size()).thenAnswer(mock -> paths.size());

        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> java.nio.file.Path.of(((Path) mock.getArguments()[0]).toUri().toString()));

        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        HdfsWorker<byte[]> worker = new HdfsWorker<>(
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
        List<Path> paths = IntStream.range(0, 1)
                .mapToObj(i -> new Path("/test" + i))
                .toList();

        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        HdfsSplit split = mock(HdfsSplit.class);
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);

        when(split.iterator()).thenAnswer(mock -> paths.iterator());

        when(split.size()).thenAnswer(mock -> paths.size());

        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> java.nio.file.Path.of(((Path) mock.getArguments()[0]).toUri().toString()));

        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        HdfsWorker<byte[]> worker = new HdfsWorker<>(
                workerEventProducer,
                fileRWFactory,
                queue,
                counterAggregator,
                split,
                fileDownloader
        );

        worker.initThread();

        assertThat(counterAggregator.getCounters()).hasSize(3);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 33})
    void test_1(int range) throws Exception {
        // given
        String threadName = "HDFS-Worker-test";
        int elementsPerFile = 5;
        List<Path> paths = IntStream.range(0, range)
                .mapToObj(i -> new Path("/test" + i))
                .toList();

        MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class);
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        HdfsSplit split = mock(HdfsSplit.class);
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);

        when(split.iterator()).thenAnswer(mock -> paths.iterator());

        when(split.size()).thenAnswer(mock -> paths.size());

        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> java.nio.file.Path.of(((Path) mock.getArguments()[0]).toUri().toString()));

        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(elementsPerFile));

        HdfsWorker<byte[]> worker = new HdfsWorker<>(
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
        fileUtilsMock.verify(() -> FileUtils.deleteFile(any(java.nio.file.Path.class)), times(paths.size()));

        verify(fileRWFactory, times(paths.size())).buildFileReader(any(PathWrapper.class));

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_files").getCount())
                .isEqualTo(paths.size());
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(elementsPerFile * paths.size());

        assertThat(queue.size()).isEqualTo(elementsPerFile * paths.size());
        StreamElement<byte[]> element;
        while ((element = queue.poll()) != null) {
            assertThat(element.value()).isNotNull();
        }

        ThreadUtils.assertNoWorkerThreadsAlive();

        // after
        fileUtilsMock.close();
    }

    @Test
    void test_emptyIterator() throws Exception {
        // given
        String threadName = "HDFS-Worker-test";
        int elementsPerFile = 5;

        MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class);
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        HdfsSplit split = mock(HdfsSplit.class);
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);

        when(split.iterator()).thenAnswer(mock -> Collections.emptyIterator());

        when(split.size()).thenAnswer(mock -> 0);

        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> java.nio.file.Path.of(((Path) mock.getArguments()[0]).toUri().toString()));

        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(elementsPerFile));

        HdfsWorker<byte[]> worker = new HdfsWorker<>(
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
        assertThat(counterAggregator.getCounter(threadName + "_read_files").getCount())
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
        String threadName = "HDFS-Worker-test";
        int elementsPerFile = 5;
        List<Path> paths = IntStream.range(0, 10)
                .mapToObj(i -> new Path("/test" + i))
                .toList();

        MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class);
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        HdfsSplit split = mock(HdfsSplit.class);
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);

        when(split.iterator()).thenAnswer(mock -> paths.iterator());

        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> {throw new Handover.ClosedException();});

        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(elementsPerFile));

        HdfsWorker<byte[]> worker = new HdfsWorker<>(
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
        assertThat(counterAggregator.getCounter(threadName + "_read_files").getCount())
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
        String threadName = "HDFS-Worker-test";
        int elementsPerFile = 5;
        List<Path> paths = IntStream.range(0, 10)
                .mapToObj(i -> new Path("/test" + i))
                .toList();

        MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class);
        WorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        FileRWFactory<byte[]> fileRWFactory = mock(FileRWFactory.class);
        BlockingQueue<StreamElement<byte[]>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        HdfsSplit split = mock(HdfsSplit.class);
        HdfsFileDownloader fileDownloader = mock(HdfsFileDownloader.class);

        when(split.iterator()).thenAnswer(mock -> paths.iterator());

        when(fileDownloader.download(any(Path.class))).thenAnswer(
                mock -> {throw new RuntimeException();});

        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(elementsPerFile));

        HdfsWorker<byte[]> worker = new HdfsWorker<>(
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
        assertThat(counterAggregator.getCounter(threadName + "_read_files").getCount())
                .isEqualTo(0);
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(0);

        assertThat(queue.size()).isEqualTo(0);
        ThreadUtils.assertNoWorkerThreadsAlive();

        // after
        fileUtilsMock.close();
    }

}
