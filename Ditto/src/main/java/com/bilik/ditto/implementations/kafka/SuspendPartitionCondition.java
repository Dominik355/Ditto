package com.bilik.ditto.implementations.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Predicate;

/**
 * Condition, which determines, if consumer should continue polling
 * records from partition of the provided record.
 */
@FunctionalInterface
public interface SuspendPartitionCondition extends Predicate<ConsumerRecord<byte[], byte[]>> {

    /**
     * DEFAULT IMPLEMENTATION
     */
    class DefaultSuspendPartitionCondition implements SuspendPartitionCondition {

        private final KafkaRecordRange range;

        public DefaultSuspendPartitionCondition(KafkaRecordRange range) {
            this.range = range;
        }

        @Override
        public boolean test(ConsumerRecord<byte[], byte[]> record) {
            return range.isTimeRange() ?
                    record.timestamp() <= range.getTo() :
                    record.offset() <= range.getTo();
        }
    }

}

