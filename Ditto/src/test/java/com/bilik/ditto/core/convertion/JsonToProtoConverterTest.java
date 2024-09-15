package com.bilik.ditto.core.convertion;

import com.google.protobuf.Message;
import com.bilik.ditto.core.convertion.factory.JsonToProtoConverterFactory;
import com.bilik.ditto.testCommons.SensorGenerator;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonToProtoConverterTest {

    @Test
    void testConvertion() throws Exception {
        var converter = new JsonToProtoConverterFactory.JsonToProtoConverter(Sensor.class);

        Message sensor = converter.convert(SensorGenerator.SENSOR_CONSTANT_JSON);

        assertThat(sensor).isInstanceOf(Sensor.class);
        assertThat(sensor).isEqualTo(SensorGenerator.SENSOR_CONSTANT);
    }


}
