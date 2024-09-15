package com.bilik.ditto.testCommons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import proto.test.Sensor;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SensorUtils {

    @SuppressWarnings("unchecked")
    public static void assertJsonFromProto(Sensor message, String json) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        final Map<String, Object> sensorMap = mapper.readValue(json, new TypeReference<>(){});
        assertEqual(sensorMap.get("name"), message.getName());
        assertEqual(sensorMap.get("humidity"), message.getHumidity());
        assertEqual(sensorMap.get("temperature"), message.getTemperature());
        assertEqual(sensorMap.get("door"), message.getDoor().name());

        final Map<String, Object> locationMap = (Map<String, Object>) sensorMap.get("location");
        assertEqual(locationMap.get("x"), message.getLocation().getX());
        assertEqual(locationMap.get("y"), message.getLocation().getY());

        final Map<String, Object> eventMap = (Map<String, Object>) sensorMap.get("event");
        assertEqual(eventMap.get("name"), message.getEvent().getName());
        assertEqual(eventMap.get("value"), message.getEvent().getValue());
    }

    /**
     * In proto, 0 values are not used. So every 0 or false is represented as null in json.
     * So we need to do additional check.
     */
    public static void assertEqual(Object valueFromJson, Object protoValue) {
        if (valueFromJson == null) {
            Class<?> protoValClass = protoValue.getClass();
            if (protoValClass.isPrimitive()) {
                if (protoValue instanceof Byte)
                    assertThat(protoValue).isEqualTo(0);
                if (protoValue instanceof Short)
                    assertThat(protoValue).isEqualTo(0);
                if (protoValue instanceof Integer)
                    assertThat(protoValue).isEqualTo(0);
                if (protoValue instanceof Long)
                    assertThat(protoValue).isEqualTo(0);
                if (protoValue instanceof Character)
                    assertThat(protoValue).isEqualTo(0);
                if (protoValue instanceof Float)
                    assertThat(protoValue).isEqualTo(0);
                if (protoValue instanceof Double)
                    assertThat(protoValue).isEqualTo(0);
                if (protoValue instanceof Boolean)
                    assertThat(protoValue).isEqualTo(false);
            }
        } else {
            assertThat(valueFromJson).isEqualTo(protoValue);
        }
    }

}
