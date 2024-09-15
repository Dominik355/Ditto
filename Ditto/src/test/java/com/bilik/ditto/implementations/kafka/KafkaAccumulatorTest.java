package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.common.collections.Tuple2;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType.ProtobufSerde;
import com.bilik.ditto.implementations.kafka.KafkaAccumulator.RoundRobinPartitioner;
import com.bilik.ditto.testCommons.SensorGenerator;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import proto.test.Sensor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaAccumulatorTest {

    private static final String TOPIC = "Test-topic";
    private static final int BATCH = 5;
    private static final List<PartitionInfo> PARTITIONS = List.of(
            new PartitionInfo(TOPIC, 0, null, null, null),
            new PartitionInfo(TOPIC, 1, null, null, null),
            new PartitionInfo(TOPIC, 2, null, null, null)
    );
    
    private TopicSpecificKafkaProducer<byte[], byte[]> specificKafkaProducer;
    private KafkaAccumulator<Sensor> accumulator;

    @BeforeEach
    void init() {
        specificKafkaProducer = mock(TopicSpecificKafkaProducer.class);
        when(specificKafkaProducer.partitions()).thenReturn(PARTITIONS);

        accumulator = new KafkaAccumulator<>(
              5,
              specificKafkaProducer,
                new ProtobufSerde<Sensor>(Sensor.class).serializer()
        );
    }

    @Test
    void isFullTest(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws IOException, InterruptedException {
        for (Sensor sensor : SensorGenerator.getRange(0, BATCH - 1)) {
            accumulator.collect(StreamElement.of(sensor));
        }
        assertThat(accumulator.isFull()).isFalse();

        accumulator.collect(StreamElement.of(SensorGenerator.getSingle()));
        assertThat(accumulator.isFull()).isTrue();
    }

    @Test
    void tetet(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws IOException, InterruptedException {
        for (Sensor sensor : SensorGenerator.getRange(0, BATCH - 1)) {
            accumulator.collect(StreamElement.of(sensor));
        }
        assertThat(accumulator.isFull()).isFalse();

        accumulator.collect(StreamElement.of(SensorGenerator.getSingle()));
        assertThat(accumulator.isFull()).isTrue();
    }

    @Test
    void test_usedOnFull() throws Exception {
        for (Sensor sensor : SensorGenerator.getRange(0, BATCH)) {
            accumulator.collect(StreamElement.of(sensor));
        }
        assertThat(accumulator.isFull()).isTrue();

        accumulator.onFull().run();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> recordCapture = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(specificKafkaProducer, times(BATCH))
                .send(recordCapture.capture(), any(Callback.class));

        assertThat(recordCapture.getValue()).isNotNull();
        assertThat(accumulator.isFull()).isFalse(); // has been emptied
    }

    @Test
    void test_usedOnFull_accumulatorNotFull() throws Exception {
        for (Sensor sensor : SensorGenerator.getRange(0, BATCH - 1)) {
            accumulator.collect(StreamElement.of(sensor));
        }
        assertThat(accumulator.isFull()).isFalse();

        accumulator.onFull().run();

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> recordCapture = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(specificKafkaProducer, times(BATCH - 1))
                .send(recordCapture.capture(), any(Callback.class));

        assertThat(recordCapture.getValue()).isNotNull();
        assertThat(accumulator.isFull()).isFalse(); // has been emptied
    }

    @Test
    void test_multipleUploads() throws Exception {
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> recordCapture = ArgumentCaptor.forClass(ProducerRecord.class);

        for (int i = 0; i < 7; i++) {
            for (Sensor sensor : SensorGenerator.getRange(0, BATCH)) {
                accumulator.collect(StreamElement.of(sensor));
            }
            assertThat(accumulator.isFull()).isTrue();

            accumulator.onFull().run();

            verify(specificKafkaProducer, times(BATCH * (i + 1)))
                    .send(recordCapture.capture(), any(Callback.class));

            assertThat(recordCapture.getValue()).isNotNull();
            assertThat(accumulator.isFull()).isFalse(); // has been emptied
        }
    }

    @Test
    void roundRobinPartitionerTest_singlePartition() {
        var partition = new PartitionInfo(TOPIC, 0, null, null, null);
        RoundRobinPartitioner partitioner = new RoundRobinPartitioner(List.of(partition));


        List<Tuple2<byte[], Long>> elements = IntStream.range(0, 3)
                .boxed()
                .map(i -> Tuple2.of(new byte[i], Long.valueOf(i)))
                .toList();

        // first
        partitioner.createProducerRecords(elements)
                .forEach(record -> assertThat(record.partition()).isZero());

        // check after shifting inner array of partitions
        partitioner.createProducerRecords(elements)
                .forEach(record -> assertThat(record.partition()).isZero());
    }

    @Test
    void roundRobinPartitionerTest_multiplePartitions() {
        RoundRobinPartitioner partitioner = new RoundRobinPartitioner(PARTITIONS);
        List<Tuple2<byte[], Long>> elements = IntStream.range(0, 1)
                .boxed()
                .map(i -> Tuple2.of(new byte[i], Long.valueOf(i)))
                .toList();

        for(int i = 0; i < PARTITIONS.size() * 3; i++) {
            int finalI = i;
            partitioner.createProducerRecords(elements)
                    .forEach(record -> assertThat(record.partition())
                            .isEqualTo(PARTITIONS.get(finalI % PARTITIONS.size()).partition()));
        }
    }
}
