package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.transfer.Handover;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType;
import com.bilik.ditto.implementations.kafka.factory.JobSpecificKafkaConsumerFactory;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import com.bilik.ditto.testCommons.ThreadUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.bilik.ditto.implementations.kafka.KafkaTestData.*;
import static com.bilik.ditto.testCommons.SensorGenerator.SENSOR_CONSTANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaWorkerTest {

    @Test
    void run_notInitialized() {
        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        JobSpecificKafkaConsumerFactory consumerFactory = mock(JobSpecificKafkaConsumerFactory.class);
        BlockingQueue<StreamElement<Sensor>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        KafkaSplit split = new KafkaSplit(PARTITIONS);

        KafkaWorker<Sensor> worker = new KafkaWorker<>(
                workerEventProducer,
                new ProtobufType<>(Sensor.class),
                queue,
                counterAggregator,
                split,
                OFFSET_RANGE,
                consumerFactory,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(worker::run)
                .withMessage("WorkerThread has not been initialized yet!");
    }

    @Test
    void initializationTest() {
        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        JobSpecificKafkaConsumerFactory consumerFactory = mock(JobSpecificKafkaConsumerFactory.class);
        BlockingQueue<StreamElement<Sensor>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);

        when(consumerFactory.createConsumer(anyString())).thenReturn(consumer);
        when(consumer.beginningOffsets(any(Collection.class))).thenReturn(BEGINNING_OFFSETS);

        KafkaWorker<Sensor> worker = new KafkaWorker<>(
                workerEventProducer,
                new ProtobufType<>(Sensor.class),
                queue,
                counterAggregator,
                split,
                OFFSET_RANGE,
                consumerFactory,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );

        assertThat(worker.initThread()).isTrue();

        assertThat(counterAggregator.getCounters()).hasSize(3 + PARTITIONS.size());
    }

    @Test
    void initializationTest_lowestOffsetBiggerThanMaxRange() {
        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        JobSpecificKafkaConsumerFactory consumerFactory = mock(JobSpecificKafkaConsumerFactory.class);
        BlockingQueue<StreamElement<Sensor>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        KafkaSplit split = new KafkaSplit(PARTITIONS);

        when(consumerFactory.createConsumer(anyString())).thenReturn(consumer);
        when(consumer.beginningOffsets(any(Collection.class))).thenReturn(Map.of(
                PARTITIONS.get(0), OFFSET_RANGE.getTo() + 1,
                PARTITIONS.get(1), OFFSET_RANGE.getTo() + 1,
                PARTITIONS.get(2), OFFSET_RANGE.getTo() + 1
        ));

        KafkaWorker<Sensor> worker = new KafkaWorker<>(
                workerEventProducer,
                new ProtobufType<>(Sensor.class),
                queue,
                counterAggregator,
                split,
                OFFSET_RANGE,
                consumerFactory,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );


        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(worker::initThread)
                .withMessage("There are no timestamps/offsets that would satisfy this job's range requirements");
    }

    @Test
    void test_1() {
        String threadName = "Kafka-Worker-test";

        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        JobSpecificKafkaConsumerFactory consumerFactory = mock(JobSpecificKafkaConsumerFactory.class);
        BlockingQueue<StreamElement<Sensor>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        KafkaSplit split = new KafkaSplit(PARTITIONS);

        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        consumer.updateBeginningOffsets(BEGINNING_OFFSETS);
        RECORDS.forEach(rec ->
                consumer.schedulePollTask(() ->
                        rec.forEach(consumer::addRecord)));

        when(consumerFactory.createConsumer(anyString())).thenReturn(consumer);

        KafkaWorker<Sensor> worker = new KafkaWorker<>(
                workerEventProducer,
                new ProtobufType<>(Sensor.class),
                queue,
                counterAggregator,
                split,
                OFFSET_RANGE,
                consumerFactory,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );
        worker.setName(threadName);

        assertThat(worker.initThread()).isTrue();
        worker.run();
        worker.shutdown();

        // then

        assertThat(counterAggregator.getCounters()).hasSize(3 + PARTITIONS.size());
        for (TopicPartition partition : split.getSplit()) {
            assertThat(counterAggregator.getCounter(threadName + "_read_records_" + partition.partition()).getCount())
                    .isEqualTo(3L);
        }

        assertThat(queue.size()).isEqualTo(PARTITIONS.size() * 3);
        StreamElement<Sensor> element;
        while ((element = queue.poll()) != null) {
            assertThat(element.value()).isEqualTo(SENSOR_CONSTANT);
        }

        for (var partition : PARTITIONS) {
            assertThat(worker.getTopicTracker().getLastOffset(partition)).isEqualTo(3);
        }

        ThreadUtils.assertNoWorkerThreadsAlive();

        assertThat(workerEventProducer.events()).hasSize(3);
        assertThat(workerEventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
        assertThat(workerEventProducer.events().get(1).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
        assertThat(workerEventProducer.events().get(2).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.FINISHED);
    }

    @Test
    void test_exceptionInConsumerThread() {
        String threadName = "Kafka-Worker-test";

        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        JobSpecificKafkaConsumerFactory consumerFactory = mock(JobSpecificKafkaConsumerFactory.class);
        BlockingQueue<StreamElement<Sensor>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        KafkaSplit split = new KafkaSplit(PARTITIONS);

        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, PARTITIONS_INFO);
        consumer.updateBeginningOffsets(BEGINNING_OFFSETS);
        consumer.schedulePollTask(() -> {throw new RuntimeException("test-exception");});

        when(consumerFactory.createConsumer(anyString())).thenReturn(consumer);

        KafkaWorker<Sensor> worker = new KafkaWorker<>(
                workerEventProducer,
                new ProtobufType<>(Sensor.class),
                queue,
                counterAggregator,
                split,
                OFFSET_RANGE,
                consumerFactory,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );
        worker.setName(threadName);

        assertThat(worker.initThread()).isTrue();
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(worker::run)
                .withMessageContaining("test-exception");
        worker.shutdown();

        // then

        assertThat(counterAggregator.getCounters()).hasSize(3 + PARTITIONS.size());
        for (TopicPartition partition : split.getSplit()) {
            assertThat(counterAggregator.getCounter(threadName + "_read_records_" + partition.partition()).getCount())
                    .isEqualTo(0);
        }

        assertThat(queue.size()).isEqualTo(0);

        for (var partition : PARTITIONS) {
            assertThat(worker.getTopicTracker().getLastOffset(partition)).isEqualTo(-1);
        }

        assertThat(workerEventProducer.events()).hasSize(3);
        assertThat(workerEventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED); //worker
        assertThat(workerEventProducer.events().get(1).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED); // consumer
        assertThat(workerEventProducer.events().get(2).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.ERROR);
        
        ThreadUtils.assertNoWorkerThreadsAlive();
    }

    @Test
    void test_handoverClosedByConsumerOnFirstPoll() {
        String threadName = "Kafka-Worker-test";

        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        JobSpecificKafkaConsumerFactory consumerFactory = mock(JobSpecificKafkaConsumerFactory.class);
        BlockingQueue<StreamElement<Sensor>> queue = new LinkedBlockingQueue<>();
        CounterAggregator counterAggregator = new CounterAggregator();
        KafkaSplit split = new KafkaSplit(PARTITIONS);

        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any(Duration.class)))
                .thenAnswer(mock -> {throw new Handover.ClosedException();});
        when(consumer.beginningOffsets(any(Collection.class)))
                .thenReturn(BEGINNING_OFFSETS);

        when(consumerFactory.createConsumer(anyString())).thenReturn(consumer);

        KafkaWorker<Sensor> worker = new KafkaWorker<>(
                workerEventProducer,
                new ProtobufType<>(Sensor.class),
                queue,
                counterAggregator,
                split,
                OFFSET_RANGE,
                consumerFactory,
                new KafkaProperties.KafkaSourceConfig("1 ms")
        );
        worker.setName(threadName);

        assertThat(worker.initThread()).isTrue();
        worker.run();
        worker.shutdown();

        // then

        assertThat(counterAggregator.getCounters()).hasSize(3 + PARTITIONS.size());
        for (TopicPartition partition : split.getSplit()) {
            assertThat(counterAggregator.getCounter(threadName + "_read_records_" + partition.partition()).getCount())
                    .isEqualTo(0);
        }

        assertThat(queue.size()).isEqualTo(0);

        for (var partition : PARTITIONS) {
            assertThat(worker.getTopicTracker().getLastOffset(partition)).isEqualTo(-1);
        }

        assertThat(workerEventProducer.events()).hasSize(3);
        assertThat(workerEventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED); //worker
        assertThat(workerEventProducer.events().get(1).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED); // consumer

        ThreadUtils.assertNoWorkerThreadsAlive();
    }

}
