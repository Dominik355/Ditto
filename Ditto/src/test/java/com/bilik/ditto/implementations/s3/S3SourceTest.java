package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType;
import com.bilik.ditto.testCommons.DummyFileReader;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import proto.test.Sensor;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.s3.model.S3Object.builder;

public class S3SourceTest {

    private static final int PARALLELISM = 3;
    private static final String JOB_ID = "job-id-test";

    private S3Source<Sensor> source;
    private Map<Integer, BlockingQueue<StreamElement<Sensor>>> queues;
    private Path tempDir;
    private S3ObjectRange range;
    private S3Handler s3Handler = mock(S3Handler.class);
    private FileRWFactory<Sensor> fileRWFactory;
    private Instant start = LocalDateTime.of(LocalDate.now(), LocalTime.of(14, 0)).toInstant(ZoneOffset.UTC);
    private Instant end = LocalDateTime.of(LocalDate.now(), LocalTime.of(18, 0)).toInstant(ZoneOffset.UTC);


    @BeforeEach
    void init(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        this.tempDir = tempDir;
        queues = IntStream.range(0, PARALLELISM)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), i -> new LinkedBlockingQueue<>()));

        FileRWInfo<Sensor> fileRWInfo = FileRWInfo.tempFileInfo(Sensor.class, tempDir);
        fileRWFactory = mock(FileRWFactory.class);
        when(fileRWFactory.buildFileReader(any(PathWrapper.class)))
                .thenAnswer(mock -> new DummyFileReader(5));

        when(fileRWFactory.getInfo()).thenReturn(fileRWInfo);

        Instant start = LocalDateTime.of(LocalDate.now(), LocalTime.of(14, 0)).toInstant(ZoneOffset.UTC);
        Instant end = LocalDateTime.of(LocalDate.now(), LocalTime.of(18, 0)).toInstant(ZoneOffset.UTC);
        range = S3ObjectRange.of(start.toEpochMilli(), end.toEpochMilli(), List.of("test"));

        source = new S3Source<>(
                PARALLELISM,
                new ProtobufType<>(Sensor.class),
                range,
                JOB_ID,
                "test-bucket",
                s3Handler,
                new MockingWorkerEventProducer(),
                fileRWFactory
        );
    }

    @Test
    void noExistingObject() {
        when(s3Handler.listObjectsIterator(anyString(), anyString()))
                .thenReturn(Collections.emptyIterator());

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() -> source.initialize(queues))
                .withMessageContaining("No object for prefix");
    }

    @Test
    void nothingMatchesTimeRange() {
        List<ListObjectsV2Response> objects = List.of(
                ListObjectsV2Response.builder().contents(
                        builder().key("test").lastModified(start.minusSeconds(1)).build(),
                        builder().key("test321").lastModified(start.minusSeconds(976)).build(),
                        builder().key("test98").lastModified(end.plusSeconds(8)).build()
                ).build()
        );
        when(s3Handler.listObjectsIterator(anyString(), anyString()))
                .thenReturn(objects.iterator());

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() -> source.initialize(queues))
                .withMessageContaining("No object for prefix");
    }

    @Test
    void lastModifiedIsEdgeOfRange() {
        List<ListObjectsV2Response> objects = List.of(
                ListObjectsV2Response.builder().contents(
                        builder().key("test").lastModified(start).build(),
                        builder().key("test98").lastModified(end).build(),
                        builder().key("testtew").lastModified(end).build(),
                        builder().key("test432").lastModified(start).build()

                ).build()
        );
        when(s3Handler.listObjectsIterator(anyString(), anyString()))
                .thenReturn(objects.iterator());

        assertThat(source.initialize(queues)).containsExactly(1, 2, 3);
        assertThat(source.getParallelism()).isEqualTo(PARALLELISM);
        assertThat(source.getObjectsTotal()).isEqualTo(4);
    }


    @Test
    void lessObjectsFoundThanParallelism() throws IOException {
        List<ListObjectsV2Response> objects = List.of(
                ListObjectsV2Response.builder().contents(
                        builder().key("test").lastModified(start).build(),
                        builder().key("test98").lastModified(end).build()

                ).build()
        );
        when(s3Handler.listObjectsIterator(anyString(), anyString()))
                .thenReturn(objects.iterator());

        assertThat(source.initialize(queues)).containsExactly(1, 2);
        assertThat(source.getParallelism()).isEqualTo(2);
        assertThat(source.getObjectsTotal()).isEqualTo(2);
    }

    @Test
    void objectCountEqualsToParallelism() throws IOException {
        List<ListObjectsV2Response> objects = List.of(
                ListObjectsV2Response.builder().contents(
                        builder().key("test").lastModified(start).build(),
                        builder().key("test98").lastModified(end).build(),
                        builder().key("testter").lastModified(end).build()
                ).build()
        );
        when(s3Handler.listObjectsIterator(anyString(), anyString()))
                .thenReturn(objects.iterator());

        assertThat(source.initialize(queues)).containsExactly(1, 2, 3);
        assertThat(source.getParallelism()).isEqualTo(PARALLELISM);
        assertThat(source.getObjectsTotal()).isEqualTo(PARALLELISM);
    }

    @Test
    void fileCountBiggerThanParallelism() throws IOException {
        List<ListObjectsV2Response> objects = List.of(
                ListObjectsV2Response.builder().contents(
                        IntStream.range(0, PARALLELISM * 5)
                                .mapToObj(i -> builder().key("test" + i).lastModified(start).build())
                                .toList()
                ).build()
        );
        when(s3Handler.listObjectsIterator(anyString(), anyString()))
                .thenReturn(objects.iterator());

        assertThat(source.initialize(queues)).containsExactly(1, 2, 3);
        assertThat(source.getParallelism()).isEqualTo(PARALLELISM);
        assertThat(source.getObjectsTotal()).isEqualTo(PARALLELISM * 5);
    }

}
