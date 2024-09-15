package com.bilik.ditto.core.io.impl;

import com.google.protobuf.Message;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.testCommons.SensorGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import proto.test.Sensor;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtobufParquetFileRWFactoryTest {

    @Test
    void getInfoTest() {
        var info = FileRWInfo.protoFileInfo(Sensor.class, Path.of("anything"));
        ProtobufParquetFileRWFactory factory = new ProtobufParquetFileRWFactory(info);

        assertThat(factory.getInfo()).isEqualTo(info);
    }

    @Test
    void readWriteTest(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        String testFilePath = "test/ProtobufParquetFileRWFactoryTest/test1";
        var info = FileRWInfo.protoFileInfo(Sensor.class, tempDir);
        ProtobufParquetFileRWFactory factory = new ProtobufParquetFileRWFactory(info);
        PathWrapper testFile = new PathWrapper(factory.getInfo().resolveFile(testFilePath));

        Set<Sensor> sensors = SensorGenerator.getRangeSet(0, 10);
        try (var fileWriter = factory.buildFileWriter(testFile);) {
            for (Message sensor : sensors) {
                fileWriter.write(sensor);
            }

            assertThat(fileWriter.writtenElements()).isEqualTo(10);
            assertThat(fileWriter.size()).isGreaterThan(10); // cant say the size, but every sensor is supposed to have at least more than 1 byte
        }

        Set<Sensor> readMessages = new HashSet<>();
        try (var fileReader = factory.buildFileReader(testFile);) {
            Sensor mes;
            while ((mes = (Sensor) fileReader.next()) != null) {
                readMessages.add(mes);
            }
        }

        assertThat(sensors).containsAll(readMessages);
    }

    @Test
    void readWriteEmptyTest(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        String testFilePath = "test/ProtobufParquetFileRWFactoryTest/test1";
        var info = FileRWInfo.protoFileInfo(Sensor.class, tempDir);
        ProtobufParquetFileRWFactory factory = new ProtobufParquetFileRWFactory(info);
        PathWrapper testFile = new PathWrapper(factory.getInfo().resolveFile(testFilePath));

        try (var fileWriter = factory.buildFileWriter(testFile);) {
            assertThat(fileWriter.writtenElements()).isZero();
            assertThat(fileWriter.size()).isZero();
        }

        Set<Sensor> readMessages = new HashSet<>();
        try (var fileReader = factory.buildFileReader(testFile);) {
            Sensor mes;
            while ((mes = (Sensor) fileReader.next()) != null) {
                readMessages.add(mes);
            }
        }

        assertThat(readMessages).isEmpty();
    }

}
