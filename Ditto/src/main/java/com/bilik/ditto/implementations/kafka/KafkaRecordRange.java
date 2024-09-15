package com.bilik.ditto.implementations.kafka;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.Range;

import java.util.Objects;
import java.util.Optional;

import static com.bilik.ditto.implementations.kafka.KafkaRecordRange.RecordRangeType.*;

/**
 * Defines range of wanted records from kafka topic.
 * You can either define offset or time range.
 *
 * What if start is not specified ? well then start is 0 and at initialization
 * phase it seeks the first offset it finds.
 */
public class KafkaRecordRange {

    private final Range<Long> range;
    private final RecordRangeType recordRangeType;

    private KafkaRecordRange(Range<Long> range, RecordRangeType recordRangeType) {
        this.range = range;
        this.recordRangeType = recordRangeType;
    }

    public static enum RecordRangeType {
        TIME, OFFSET;

        public static Optional<RecordRangeType> fromString(String typeString) {
            return Optional.ofNullable(EnumUtils.getEnum(RecordRangeType.class, typeString));
        }
    }

    public static KafkaRecordRange timeRange(long from, long to) {
        return new KafkaRecordRange(Range.between(from, to), TIME);
    }

    public static KafkaRecordRange offsetRange(long from, long to) {
        return new KafkaRecordRange(Range.between(from, to), OFFSET);
    }

    public static KafkaRecordRange range(long from, long to, RecordRangeType type) {
        return new KafkaRecordRange(Range.between(from, to), type);
    }

    public static KafkaRecordRange range(long from, long to, String type) {
        return range(from, to,
                RecordRangeType.fromString(type)
                        .orElseThrow(() -> new IllegalArgumentException(type + " - is not a valid RecordRangeType")));
    }

    public boolean isTimeRange() {
        return TIME.equals(recordRangeType);
    }

    public boolean isOffsetRange() {
        return OFFSET.equals(recordRangeType);
    }

    public RecordRangeType getType() {
        return isTimeRange() ? TIME : OFFSET;
    }

    public long getFrom() {
        return this.range.getMinimum();
    }

    public long getTo() {
        return this.range.getMaximum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaRecordRange that = (KafkaRecordRange) o;
        return recordRangeType.equals(that.recordRangeType) && range.equals(that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(range, recordRangeType);
    }

    @Override
    public String toString() {
        return (isTimeRange() ? "[Time]" : "[Offset]") + "KafkaRecordRange(" + getFrom() + " - " + getTo() + ")";
    }
}
