package com.bilik.ditto.core.convertion;

import com.bilik.ditto.core.convertion.factory.ProtoToJsonConverterFactory;
import org.junit.jupiter.api.Test;

import static com.bilik.ditto.testCommons.SensorGenerator.SENSOR_CONSTANT;
import static com.bilik.ditto.testCommons.SensorUtils.assertJsonFromProto;

public class ProtoToJsonConverterTest {

    @Test
    void convertTest() throws Exception {
        var converter = new ProtoToJsonConverterFactory.ProtoToJsonConverter();

        String json = converter.convert(SENSOR_CONSTANT);

        assertJsonFromProto(SENSOR_CONSTANT, json);
    }

}
