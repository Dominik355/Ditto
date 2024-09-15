package com.bilik.ditto.core.job;

import com.bilik.ditto.testCommons.SensorGenerator;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StreamElementTest {

    @Test
    void constructorNullValue_1() {
        assertThatThrownBy(() -> new StreamElement<>(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorNullValue_2() {
        assertThatThrownBy(() -> new StreamElement<>(null, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorNullValue_3() {
        var element = new StreamElement<>(null, true);
        assertThat(element.isLast()).isTrue();
        assertThat(element.value()).isNull();
    }

    @Test
    void constructorNullValue_4() {
        assertThatThrownBy(() -> new StreamElement<>(null, false, 123L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void staticMethodLast() {
        var element = StreamElement.last();
        assertThat(element.isLast()).isTrue();
        assertThat(element.value()).isNull();
    }

    @Test
    void staticMethodOf_Value_notLast() {
        Sensor sensor = SensorGenerator.getSingle();
        var element = StreamElement.of(sensor);
        assertThat(element.isLast()).isFalse();
        assertThat(element.value()).isEqualTo(sensor);
        assertThat(element.timestamp()).isEqualTo(-1);
    }

    @Test
    void staticMethodOf_Value_notLast_timestamp() {
        Sensor sensor = SensorGenerator.getSingle();
        var element = StreamElement.of(sensor, 123L);
        assertThat(element.isLast()).isFalse();
        assertThat(element.value()).isEqualTo(sensor);
        assertThat(element.timestamp()).isEqualTo(123L);
    }

    @Test
    void staticMethodOf_originElement() {
        Sensor originalSensor = SensorGenerator.getSingle();
        var originalElement = StreamElement.of(originalSensor, 1234L);

        Sensor newSensor = SensorGenerator.getSingle();
        var newElement = StreamElement.of(newSensor, originalElement);

        assertThat(newElement.isLast()).isEqualTo(originalElement.isLast());
        assertThat(newElement.value()).isEqualTo(newSensor);
        assertThat(newElement.timestamp()).isEqualTo(originalElement.timestamp());
    }

}
