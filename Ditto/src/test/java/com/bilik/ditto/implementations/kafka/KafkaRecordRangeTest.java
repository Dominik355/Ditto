package com.bilik.ditto.implementations.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaRecordRangeTest {

    @Test
    void testTime_1() {
        KafkaRecordRange range = KafkaRecordRange.range(0, 100, KafkaRecordRange.RecordRangeType.TIME);

        assertThat(range.isOffsetRange()).isFalse();
        assertThat(range.isTimeRange()).isTrue();
        assertThat(range.getFrom()).isEqualTo(0);
        assertThat(range.getTo()).isEqualTo(100);
    }

    @Test
    void testTime_2() {
        KafkaRecordRange range = KafkaRecordRange.timeRange(0, 100);

        assertThat(range.isOffsetRange()).isFalse();
        assertThat(range.isTimeRange()).isTrue();
        assertThat(range.getFrom()).isEqualTo(0);
        assertThat(range.getTo()).isEqualTo(100);
    }

    @Test
    void testOffset_1() {
        KafkaRecordRange range = KafkaRecordRange.range(0, 100, KafkaRecordRange.RecordRangeType.OFFSET);

        assertThat(range.isOffsetRange()).isTrue();
        assertThat(range.isTimeRange()).isFalse();
        assertThat(range.getFrom()).isEqualTo(0);
        assertThat(range.getTo()).isEqualTo(100);
    }

    @Test
    void testOffset_2() {
        KafkaRecordRange range = KafkaRecordRange.offsetRange(0, 100);

        assertThat(range.isOffsetRange()).isTrue();
        assertThat(range.isTimeRange()).isFalse();
        assertThat(range.getFrom()).isEqualTo(0);
        assertThat(range.getTo()).isEqualTo(100);
    }
}
