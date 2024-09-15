package com.bilik.ditto.core.job.output.accumulation;

import com.google.protobuf.Message;
import com.bilik.ditto.core.common.DataSize;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.FileWriter;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.io.impl.ProtobufParquetFileRWFactory;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.job.output.accumulation.FileAccumulator.DataSizeCondition;
import com.bilik.ditto.core.job.output.accumulation.FileAccumulator.ElementCountCondition;
import com.bilik.ditto.testCommons.SensorGenerator;
import com.bilik.ditto.implementations.local.LocalFileUploader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import proto.test.Sensor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileAccumulatorTest {

    static final String JOB_ID = "test-job-0";

    @Test
    void DataSizeConditionTest() throws IOException {
        DataSizeCondition condition = new DataSizeCondition(DataSize.ofBytes(100));
        DummyFileWriter fileWriter = new DummyFileWriter();

        assertThat(condition.test(fileWriter)).isFalse();

        fileWriter.write(new byte[50]);
        assertThat(condition.test(fileWriter)).isFalse();

        fileWriter.write(new byte[50]);
        assertThat(condition.test(fileWriter)).isTrue();
    }

    @Test
    void ElementCountConditionTest() throws IOException {
        ElementCountCondition condition = new ElementCountCondition(3);
        DummyFileWriter fileWriter = new DummyFileWriter();

        assertThat(condition.test(fileWriter)).isFalse();

        fileWriter.write(new byte[50]);
        assertThat(condition.test(fileWriter)).isFalse();

        fileWriter.write(new byte[50]);
        assertThat(condition.test(fileWriter)).isFalse();

        fileWriter.write(new byte[50]);
        assertThat(condition.test(fileWriter)).isTrue();
    }

    @Test
    void test_fullHandling(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws IOException, InterruptedException {
        ProtobufParquetFileRWFactory fileRWFactory = new ProtobufParquetFileRWFactory(
                FileRWInfo.protoFileInfo(Sensor.class, tempDir));
        FileUploader fileUploader = new LocalFileUploader(tempDir);
        int capacity = 5;

        FileAccumulator<Message> accumulator = new FileAccumulator<>(
                fileRWFactory,
                new ElementCountCondition(capacity),
                fileUploader,
                JOB_ID,
                1
        );

        for (Sensor sensor : SensorGenerator.getRange(0, capacity - 1)) {
            accumulator.collect(StreamElement.of(sensor));
        }
        assertThat(accumulator.isFull()).isFalse();

        accumulator.collect(StreamElement.of(SensorGenerator.getSingle()));
        assertThat(accumulator.isFull()).isTrue();
    }

    @Test
    void test_usedOnFull(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        ProtobufParquetFileRWFactory fileRWFactory = new ProtobufParquetFileRWFactory(
                FileRWInfo.protoFileInfo(Sensor.class, tempDir));
        FileUploader fileUploader = Mockito.mock(FileUploader.class);
        when(fileUploader.upload(any(PathWrapper.class))).thenAnswer(mock -> true);
        int capacity = 5;
        int workerId = 1;

        FileAccumulator<Message> accumulator = new FileAccumulator<>(
                fileRWFactory,
                new ElementCountCondition(capacity),
                fileUploader,
                JOB_ID,
                workerId
        );

        for (Sensor sensor : SensorGenerator.getRange(0, capacity - 1)) {
            accumulator.collect(StreamElement.of(sensor));
        }
        assertThat(accumulator.isFull()).isFalse();

        accumulator.collect(StreamElement.of(SensorGenerator.getSingle()));
        assertThat(accumulator.isFull()).isTrue();

        accumulator.onFull().run();

        ArgumentCaptor<PathWrapper> uploaderCaptor = ArgumentCaptor.forClass(PathWrapper.class);
        verify(fileUploader, times(1)).upload(uploaderCaptor.capture());

        assertThat(uploaderCaptor.getValue().getNioPath())
                .isEqualTo(tempDir.resolve(JOB_ID + "/sink/" + workerId + "/" + JOB_ID + "-" + workerId + "-0.parquet"));
        assertThat(accumulator.isFull()).isFalse(); // has been emptied
    }

    @Test
    void test_usedOnFull_accumulatorNotFull(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        ProtobufParquetFileRWFactory fileRWFactory = new ProtobufParquetFileRWFactory(
                FileRWInfo.protoFileInfo(Sensor.class, tempDir));
        FileUploader fileUploader = Mockito.mock(FileUploader.class);
        when(fileUploader.upload(any(PathWrapper.class))).thenAnswer(mock -> true);
        int capacity = 5;
        int workerId = 1;

        FileAccumulator<Message> accumulator = new FileAccumulator<>(
                fileRWFactory,
                new ElementCountCondition(capacity),
                fileUploader,
                JOB_ID,
                workerId
        );

        for (Sensor sensor : SensorGenerator.getRange(0, capacity - 1)) {
            accumulator.collect(StreamElement.of(sensor));
        }
        assertThat(accumulator.isFull()).isFalse();

        accumulator.onFull().run();

        ArgumentCaptor<PathWrapper> uploaderCaptor = ArgumentCaptor.forClass(PathWrapper.class);
        verify(fileUploader, times(1)).upload(uploaderCaptor.capture());

        assertThat(uploaderCaptor.getValue().getNioPath())
                .isEqualTo(tempDir.resolve(JOB_ID + "/sink/" + workerId + "/" + JOB_ID + "-" + workerId + "-0.parquet"));
        assertThat(accumulator.isFull()).isFalse(); // has been emptied
    }

    @Test
    void test_multipleUploads(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        ProtobufParquetFileRWFactory fileRWFactory = new ProtobufParquetFileRWFactory(
                FileRWInfo.protoFileInfo(Sensor.class, tempDir));
        FileUploader fileUploader = Mockito.mock(FileUploader.class);
        when(fileUploader.upload(any(PathWrapper.class))).thenAnswer(mock -> true);
        int capacity = 5;
        int workerId = 1;

        FileAccumulator<Message> accumulator = new FileAccumulator<>(
                fileRWFactory,
                new ElementCountCondition(capacity),
                fileUploader,
                JOB_ID,
                workerId
        );

        for (int i = 0; i < 7; i++) {
            for (Sensor sensor : SensorGenerator.getRange(0, capacity)) {
                accumulator.collect(StreamElement.of(sensor));
            }
            assertThat(accumulator.isFull()).isTrue();

            accumulator.onFull().run();

            ArgumentCaptor<PathWrapper> uploaderCaptor = ArgumentCaptor.forClass(PathWrapper.class);
            verify(fileUploader, times(i + 1)).upload(uploaderCaptor.capture());

            assertThat(uploaderCaptor.getValue().getNioPath()).isEqualTo(tempDir.resolve(
                    JOB_ID + "/sink/" + workerId + "/" + JOB_ID + "-" + workerId + "-" + i +".parquet"));
            assertThat(accumulator.isFull()).isFalse(); // has been emptied
        }
    }

    static class DummyFileWriter implements FileWriter<byte[]> {

        final List<byte[]> elements = new LinkedList<>();

        @Override
        public long size() throws IOException {
            return elements.stream()
                    .mapToInt(arr -> arr.length)
                    .sum();
        }

        @Override
        public void write(byte[] element) throws IOException {
            elements.add(element);
        }

        @Override
        public long writtenElements() {
            return elements.size();
        }

        @Override
        public void close() throws IOException {

        }
    }

}
