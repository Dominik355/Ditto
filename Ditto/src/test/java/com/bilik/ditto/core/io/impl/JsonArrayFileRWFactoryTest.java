package com.bilik.ditto.core.io.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.testCommons.SensorGenerator;
import com.bilik.ditto.core.util.ProtoUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import proto.test.Sensor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonArrayFileRWFactoryTest {

    @Test
    void getInfoTest() {
        var info = FileRWInfo.jsonFileInfo(Path.of("anything"));
        JsonArrayFileRWFactory factory = new JsonArrayFileRWFactory(info);

        assertThat(factory.getInfo()).isEqualTo(info);
    }

    @Test
    void readWriteTest(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        String testFilePath = "test/JsonArrayFileRWFactoryTest/test1";
        var info = FileRWInfo.jsonFileInfo(tempDir);
        JsonArrayFileRWFactory factory = new JsonArrayFileRWFactory(info);
        PathWrapper testFile = new PathWrapper(factory.getInfo().resolveFile(testFilePath));

        List<String> sensorJsonList = new ArrayList<>();
        for (Sensor sensor1 : SensorGenerator.getRange(0, 10)) {
            String json = ProtoUtils.toJson(sensor1);
            sensorJsonList.add(json);
        }

        try (var fileWriter = factory.buildFileWriter(testFile);) {
            for (String sensorJson : sensorJsonList) {
                fileWriter.write(sensorJson);
            }

            assertThat(fileWriter.writtenElements()).isEqualTo(10);
            assertThat(fileWriter.size()).isGreaterThan(10); // cant say the size, but every sensor is supposed to have at least more than 1 byte
        }

        List<String> readElements = new ArrayList<>();
        try (var fileReader = factory.buildFileReader(testFile);) {
            String sensor;
            while ((sensor = fileReader.next()) != null) {
                readElements.add(sensor);
            }
        }

        assertJsonStringCollections(sensorJsonList, readElements);
    }

    @Test
    void readWriteEmptyTest(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws Exception {
        String testFilePath = "test/JsonArrayFileRWFactoryTest/test1";
        var info = FileRWInfo.jsonFileInfo(tempDir);
        JsonArrayFileRWFactory factory = new JsonArrayFileRWFactory(info);
        PathWrapper testFile = new PathWrapper(factory.getInfo().resolveFile(testFilePath));

        try (var fileWriter = factory.buildFileWriter(testFile);) {
            assertThat(fileWriter.writtenElements()).isZero();
            assertThat(fileWriter.size()).isEqualTo(2); // empty array symbols
        }

        List<String> readElements = new ArrayList<>();
        try (var fileReader = factory.buildFileReader(testFile);) {
            String sensor;
            while ((sensor = fileReader.next()) != null) {
                readElements.add(sensor);
            }
        }

        assertThat(readElements).isEmpty();
    }

    private void assertJsonStringCollections(Collection<String> col1, Collection<String> col2) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        Set<JsonNode> nodes1 = new HashSet<>();
        for (String s : col1) {
            JsonNode readTree = objectMapper.readTree(s);
            nodes1.add(readTree);
        }

        Set<JsonNode> nodes2 = new HashSet<>();
        for (String s : col2) {
            JsonNode readTree = objectMapper.readTree(s);
            nodes2.add(readTree);
        }

        Assertions.assertThat(nodes1).containsAll(nodes2);
    }
    
}
