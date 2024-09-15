package com.bilik.ditto.core.convertion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bilik.ditto.core.convertion.factory.ProtoToFlattedJsonConverterFactory;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bilik.ditto.testCommons.SensorGenerator.SENSOR_CONSTANT;
import static org.assertj.core.api.Assertions.assertThat;

public class ProtoToFlattedJsonConverterTest {

    @Test
    void testJsonFlatMapConvertionFromProtobuf() throws Exception {
        var converter = new ProtoToFlattedJsonConverterFactory.ProtoToFlattedJsonConverter(
                getFlatMapJsonDescription(),
                Sensor.newBuilder());

        String flatenned = converter.convert(SENSOR_CONSTANT);

        ObjectMapper mapper = new ObjectMapper();
        final Map<String, Object> sensorMap = mapper.readValue(flatenned, new TypeReference<>(){});
        assertThat(sensorMap.get("name_new")).isEqualTo(SENSOR_CONSTANT.getName());
        assertThat(sensorMap.get("humidity_new")).isEqualTo(SENSOR_CONSTANT.getHumidity());
        assertThat(sensorMap.get("door_state")).isEqualTo(SENSOR_CONSTANT.getDoor().name());
        assertThat(sensorMap.get("location_x")).isEqualTo(SENSOR_CONSTANT.getLocation().getX());
        assertThat(sensorMap.get("event_name")).isEqualTo(SENSOR_CONSTANT.getEvent().getName());
        assertThat(sensorMap.get("event_value")).isEqualTo(SENSOR_CONSTANT.getEvent().getValue());
    }

    static String getFlatMapJsonDescription() throws IOException {
        String fullPath = "JsonFlatMaps/Sensor_flatmap.json";
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fullPath);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }
}
