package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.transfer.Handover;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bilik.ditto.implementations.kafka.KafkaTestData.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class KafkaConsumerThreadTest {

    @Test
    void nana() {
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(BEGINNING_OFFSETS);
        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() ->
                        rec.forEach(kafkaConsumer::addRecord)));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        // when
        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(consumer::run)
                .withMessage("WorkerThread has not been initialized yet!");
    }

    @Test
    void initializationTest_lowestOffsetBiggerThanMaxRange() {
        // given
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getTo() + 1,
                PARTITIONS.get(1), OFFSET_RANGE.getTo() + 1,
                PARTITIONS.get(2), OFFSET_RANGE.getTo() + 1
        ));
        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() -> {
                        polls.getAndIncrement();
                        rec.forEach(kafkaConsumer::addRecord);
                }));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        // when
        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(consumer::initThread)
                .withMessage("There are no timestamps/offsets that would satisfy this job's range requirements");
    }

    @Test
    void initializationTest_noOffsetExists() {
        // given
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() -> {
                    polls.getAndIncrement();
                    rec.forEach(kafkaConsumer::addRecord);
                }));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        // when
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(consumer::initThread)
                .withMessageContaining("does not have a beginning offset");
    }

    @Test
    void initializationTest_lowestTimestampBiggerThanMaxRange() {
        // given
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumerLowestTimestampBiggerThanMaxRange<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), TIMESTAMP_RANGE.getTo() + 1,
                PARTITIONS.get(1), TIMESTAMP_RANGE.getTo() + 1,
                PARTITIONS.get(2), TIMESTAMP_RANGE.getTo() + 1
        ));
        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() -> {
                    polls.getAndIncrement();
                    rec.forEach(kafkaConsumer::addRecord);
                }));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                TIMESTAMP_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        // when
        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(consumer::initThread)
                .withMessage("There are no timestamps/offsets that would satisfy this job's range requirements");
    }

    @Test
    void initializationTest_lowestOffsetSameAsMaxRange() {
        // given
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getTo(),
                PARTITIONS.get(1), OFFSET_RANGE.getTo(),
                PARTITIONS.get(2), OFFSET_RANGE.getTo()
        ));
        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() -> {
                    polls.getAndIncrement();
                    rec.forEach(kafkaConsumer::addRecord);
                }));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        // when
        assertThat(consumer.initThread()).isTrue();
        assertThat(counterAggregator.getCounters()).hasSize(3);
        for (TopicPartition partition : PARTITIONS) {
            assertThat(kafkaConsumer.position(partition)).isEqualTo(OFFSET_RANGE.getTo());
        }
    }

    @Test
    void initializationTest_lowestOffsetlowerAsMAxButHigherAsLowest() {
        // given
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getTo() - 1,
                PARTITIONS.get(1), OFFSET_RANGE.getTo() - 1,
                PARTITIONS.get(2), OFFSET_RANGE.getTo() - 1
        ));
        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() -> {
                    polls.getAndIncrement();
                    rec.forEach(kafkaConsumer::addRecord);
                }));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        // when
        assertThat(consumer.initThread()).isTrue();
        assertThat(counterAggregator.getCounters()).hasSize(3);
        for (TopicPartition partition : PARTITIONS) {
            assertThat(kafkaConsumer.position(partition)).isEqualTo(OFFSET_RANGE.getTo() - 1);
        }
    }

    @Test
    void initializationTest_lowestOffsetlowerAsLowest() {
        // given
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getFrom() - 1,
                PARTITIONS.get(1), OFFSET_RANGE.getFrom() - 1,
                PARTITIONS.get(2), OFFSET_RANGE.getFrom() - 1
        ));
        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() -> {
                    polls.getAndIncrement();
                    rec.forEach(kafkaConsumer::addRecord);
                }));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        // when
        assertThat(consumer.initThread()).isTrue();
        assertThat(counterAggregator.getCounters()).hasSize(3);
        for (TopicPartition partition : PARTITIONS) {
            assertThat(kafkaConsumer.position(partition)).isEqualTo(OFFSET_RANGE.getFrom());
        }
    }

    @Test
    void test_allWorks() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        String threadName = "Kafka-Worker-test";
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getFrom(),
                PARTITIONS.get(1), OFFSET_RANGE.getFrom(),
                PARTITIONS.get(2), OFFSET_RANGE.getFrom()
        ));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );
        consumer.setName(threadName);

        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() -> {
                    // control assertion
                    for (TopicPartition partition : PARTITIONS) {
                        assertThat(kafkaConsumer.position(partition)).isEqualTo(OFFSET_RANGE.getFrom() + polls.get());
                    }

                    polls.getAndIncrement();
                    rec.forEach(kafkaConsumer::addRecord);
                    // consumer is stopped by its KafkaWorker thread, so we need to do it excplicitly here
                    if (polls.get() > 3) {
                        consumer.unassignPartitions(PARTITIONS);
                    }
                }));

        // when
        assertThat(consumer.initThread()).isTrue();
        consumer.run();

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(PARTITIONS.size() * 3L);
        assertThat(counterAggregator.getCounter(threadName + "_empty_polls").getCount()).isZero();

        ArgumentCaptor<ConsumerRecords<byte[], byte[]>> handoverCaptor = ArgumentCaptor.forClass(ConsumerRecords.class);
        verify(handover, times(3)).produce(handoverCaptor.capture());
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);

        assertThat(kafkaConsumer.closed()).isTrue();
    }

    @Test
    void test_exceptionOccured_shouldStopButNotThrow() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        String threadName = "Kafka-Worker-test";
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getFrom(),
                PARTITIONS.get(1), OFFSET_RANGE.getFrom(),
                PARTITIONS.get(2), OFFSET_RANGE.getFrom()
        ));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );
        consumer.setName(threadName);

        kafkaConsumer.schedulePollTask(() -> {throw new DittoRuntimeException("test exception");});

        // when
        assertThat(consumer.initThread()).isTrue();
        consumer.run();

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount()).isZero();
        assertThat(counterAggregator.getCounter(threadName + "_empty_polls").getCount()).isZero();

        ArgumentCaptor<ConsumerRecords<byte[], byte[]>> handoverCaptor = ArgumentCaptor.forClass(ConsumerRecords.class);
        verify(handover, times(0)).produce(handoverCaptor.capture());
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(1)).reportError(any(DittoRuntimeException.class));

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);

        assertThat(kafkaConsumer.closed()).isTrue();
    }
    @Test
    void test_unassignPartitionsWhenPolling_shouldBeWokenUpAndPollNothing() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        String threadName = "Kafka-Worker-test";
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getFrom(),
                PARTITIONS.get(1), OFFSET_RANGE.getFrom(),
                PARTITIONS.get(2), OFFSET_RANGE.getFrom()
        ));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );
        consumer.setName(threadName);

        // when
        assertThat(consumer.initThread()).isTrue();

        RECORDS.forEach(rec -> {
            rec.stream().forEach(kafkaConsumer::addRecord);
            kafkaConsumer.schedulePollTask(() -> {
                consumer.unassignPartitions(List.of(PARTITIONS.get(polls.get())));
                polls.getAndIncrement();
            });});

        consumer.run();

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(0);
        assertThat(counterAggregator.getCounter(threadName + "_empty_polls").getCount()).isZero();

        ArgumentCaptor<ConsumerRecords<byte[], byte[]>> handoverCaptor = ArgumentCaptor.forClass(ConsumerRecords.class);
        verify(handover, times(0)).produce(handoverCaptor.capture());
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);

        assertThat(kafkaConsumer.closed()).isTrue();
    }

    @Test
    void test_unassignPartitionsAfterFirstPoll() throws Handover.ClosedException, InterruptedException, Handover.WakeupException {
        // given
        String threadName = "Kafka-Worker-test";
        AtomicInteger polls = new AtomicInteger();
        MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        Handover<ConsumerRecords<byte[],byte[]>> handover = mock(Handover.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);
        CounterAggregator counterAggregator = new CounterAggregator();

        MockConsumer<byte[], byte[]> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        kafkaConsumer.updateBeginningOffsets(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getFrom(),
                PARTITIONS.get(1), OFFSET_RANGE.getFrom(),
                PARTITIONS.get(2), OFFSET_RANGE.getFrom()
        ));

        KafkaConsumerThread consumer = new KafkaConsumerThread(
                eventProducer,
                kafkaConsumer,
                handover,
                split,
                OFFSET_RANGE,
                counterAggregator,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );
        consumer.setName(threadName);

        RECORDS.forEach(rec ->
                kafkaConsumer.schedulePollTask(() -> {
                    polls.getAndIncrement();
                    rec.forEach(kafkaConsumer::addRecord);
                }));

        doAnswer(invocation -> {
            consumer.unassignPartitions(PARTITIONS);
            return null;
        }).when(handover).produce(any(ConsumerRecords.class));

        // when
        assertThat(consumer.initThread()).isTrue();
        consumer.run();

        assertThat(counterAggregator.getCounters()).hasSize(3);
        assertThat(counterAggregator.getCounter(threadName + "_read_records").getCount())
                .isEqualTo(3L);
        assertThat(counterAggregator.getCounter(threadName + "_empty_polls").getCount()).isZero();

        ArgumentCaptor<ConsumerRecords<byte[], byte[]>> handoverCaptor = ArgumentCaptor.forClass(ConsumerRecords.class);
        verify(handover, times(1)).produce(handoverCaptor.capture());
        verify(handover, times(1)).closeSoftly();
        verify(handover, times(0)).reportError(any(Exception.class));

        assertThat(eventProducer.events()).hasSize(1);
        assertThat(eventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);

        assertThat(kafkaConsumer.closed()).isTrue();
    }

    public static class MockConsumerLowestTimestampBiggerThanMaxRange<K, V> extends MockConsumer<K, V> {

        public MockConsumerLowestTimestampBiggerThanMaxRange(OffsetResetStrategy offsetResetStrategy) {
            super(offsetResetStrategy);
        }

        @Override
        public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
            return Map.of(
                    PARTITIONS.get(0), new OffsetAndTimestamp(1, 1_001),
                    PARTITIONS.get(1), new OffsetAndTimestamp(1, 1_001),
                    PARTITIONS.get(2), new OffsetAndTimestamp(1, 1_001)
            );
        }
    }
}
