package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.exception.InvalidUserInputException;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType;
import com.bilik.ditto.testCommons.DummyFileReader;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import proto.test.Sensor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HdfsSourceTest {

    private static final int PARALLELISM = 3;
    private static final String JOB_ID = "job-id-test";

    private HdfsSource<Sensor> source;
    private Map<Integer, BlockingQueue<StreamElement<Sensor>>> queues;
    private Path tempDir;
    private HdfsFileRange fileRange;

    @BeforeEach
    void init(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        this.tempDir = tempDir;
        queues = IntStream.range(0, PARALLELISM)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), i -> new LinkedBlockingQueue<>()));

        FileRWInfo<Sensor> fileRWInfo = FileRWInfo.tempFileInfo(Sensor.class, tempDir);
        FileRWFactory<Sensor> fileRWFactory = mock(FileRWFactory.class);
        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        when(fileRWFactory.getInfo()).thenReturn(fileRWInfo);

        fileRange = mock(HdfsFileRange.class);
        when(fileRange.getParentPath()).thenReturn(new org.apache.hadoop.fs.Path(tempDir.toUri()));
        when(fileRange.getStringParentPath()).thenReturn(tempDir.toString());
        when(fileRange.fulfillsPredicates(any(LocatedFileStatus.class))).thenReturn(true);
        when(fileRange.isRecursive()).thenReturn(true);


        WorkerEventProducer eventProducer = new MockingWorkerEventProducer();

        source = new HdfsSource<>(
                PARALLELISM,
                new ProtobufType<>(Sensor.class),
                fileRange,
                JOB_ID,
                eventProducer,
                fileRWFactory,
                FileSystem.getLocal(new Configuration())
        );
    }


    @Test
    void noExistingPath() {
        when(fileRange.getParentPath()).thenReturn(new org.apache.hadoop.fs.Path("/not/existing/path/for/SURE"));

        assertThatExceptionOfType(InvalidUserInputException.class)
                .isThrownBy(() -> source.initialize(queues))
                .withMessageContaining("does not exist in defined HDFS cluster");
    }

    @Test
    void givenPathIsFile() throws IOException {
        createTempFile(0);

        Collection<Integer> workers = source.initialize(queues);

        assertThat(source.getParallelism()).isOne();
        assertThat(workers).containsExactly(1);
        assertThat(source.getPathsTotal()).isEqualTo(1);
    }

    @Test
    void noFilesFoundOnGivenPath() {
        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() -> source.initialize(queues))
                .withMessageContaining("No file for criteria ");
    }


    @Test
    void lessFilesFoundThanParallelism() throws IOException {
        for (int i = 0; i < PARALLELISM - 1; i++) {
            createTempFile(i);
        }

        Collection<Integer> workers = source.initialize(queues);

        assertThat(source.getParallelism()).isEqualTo(PARALLELISM - 1);
        assertThat(workers).containsAll(IntStream.rangeClosed(1, PARALLELISM - 1).boxed().toList());
        assertThat(source.getPathsTotal()).isEqualTo(PARALLELISM - 1);
    }

    @Test
    void fileCountEqualsToParallelism() throws IOException {
        for (int i = 0; i < PARALLELISM; i++) {
            createTempFile(i);
        }

        Collection<Integer> workers = source.initialize(queues);

        assertThat(workers).containsAll(IntStream.rangeClosed(1, PARALLELISM).boxed().toList());
        assertThat(source.getPathsTotal()).isEqualTo(PARALLELISM);
    }

    @Test
    void fileCountBiggerThanParallelism() throws IOException {
        for (int i = 0; i < PARALLELISM * 5; i++) {
            createTempFile(i);
        }

        Collection<Integer> workers = source.initialize(queues);

        assertThat(workers).containsAll(IntStream.rangeClosed(1, PARALLELISM).boxed().toList());
        assertThat(source.getPathsTotal()).isEqualTo(PARALLELISM * 5);
    }

    private Path createTempFile(int num) throws IOException {
        return Files.createTempFile(tempDir, num + "-test", ".tmp");
    }

}
