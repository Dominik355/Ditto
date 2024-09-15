package com.bilik.ditto.implementations.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

import static com.bilik.ditto.testCommons.SensorGenerator.SENSOR_CONSTANT;

public class KafkaTestData {

    public static final String TOPIC = "Test-topic";

    public static final List<TopicPartition> PARTITIONS = List.of(
            new TopicPartition(TOPIC, 0),
            new TopicPartition(TOPIC, 1),
            new TopicPartition(TOPIC, 2)
    );

    public static final List<PartitionInfo> PARTITIONS_INFO = List.of(
            new PartitionInfo(TOPIC, 0, null, null, null),
            new PartitionInfo(TOPIC, 1, null, null, null),
            new PartitionInfo(TOPIC, 2, null, null, null)
    );

    public static final KafkaRecordRange OFFSET_RANGE = KafkaRecordRange.offsetRange(1, 3);
    public static final KafkaRecordRange TIMESTAMP_RANGE = KafkaRecordRange.timeRange(100, 1_000);


    public static final List<List<ConsumerRecord<byte[], byte[]>>> RECORDS = List.of(
            List.of(
                    new ConsumerRecord<>(TOPIC, 0, 1, new byte[1], SENSOR_CONSTANT.toByteArray()),
                    new ConsumerRecord<>(TOPIC, 1, 1, new byte[1], SENSOR_CONSTANT.toByteArray()),
                    new ConsumerRecord<>(TOPIC, 2, 1, new byte[1], SENSOR_CONSTANT.toByteArray())
            ),
            List.of(
                    new ConsumerRecord<>(TOPIC, 0, 2, new byte[1], SENSOR_CONSTANT.toByteArray()),
                    new ConsumerRecord<>(TOPIC, 1, 2, new byte[1], SENSOR_CONSTANT.toByteArray()),
                    new ConsumerRecord<>(TOPIC, 2, 2, new byte[1], SENSOR_CONSTANT.toByteArray())
            ),
            List.of(
                    new ConsumerRecord<>(TOPIC, 0, 3, new byte[1], SENSOR_CONSTANT.toByteArray()),
                    new ConsumerRecord<>(TOPIC, 1, 3, new byte[1], SENSOR_CONSTANT.toByteArray()),
                    new ConsumerRecord<>(TOPIC, 2, 3, new byte[1], SENSOR_CONSTANT.toByteArray())
            ),
            List.of(
                    new ConsumerRecord<>(TOPIC, 0, 4, new byte[1], SENSOR_CONSTANT.toByteArray()),
                    new ConsumerRecord<>(TOPIC, 1, 4, new byte[1], SENSOR_CONSTANT.toByteArray()),
                    new ConsumerRecord<>(TOPIC, 2, 4, new byte[1], SENSOR_CONSTANT.toByteArray())
            )
    );

    public static final Map<TopicPartition, Long> BEGINNING_OFFSETS = Map.of(
            PARTITIONS.get(0), 1L,
            PARTITIONS.get(1), 1L,
            PARTITIONS.get(2), 1L
    );
    
}
