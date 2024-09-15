package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType;
import com.bilik.ditto.implementations.kafka.factory.JobSpecificKafkaConsumerFactory;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import proto.test.Sensor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.bilik.ditto.implementations.kafka.KafkaTestData.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaSourceTest {

    private static final int PARALLELISM = 3;
    private static final String JOB_ID = "job-id-test";

    private KafkaSource<Sensor> source;
    private Map<Integer, BlockingQueue<StreamElement<Sensor>>> queues;
    private JobSpecificKafkaConsumerFactory consumerFactory;
    private Consumer<byte[], byte[]> internalConsumer;

    @BeforeEach
    void init() {
        consumerFactory = mock(JobSpecificKafkaConsumerFactory.class);
        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        internalConsumer = mock(Consumer.class);

        when(consumerFactory.createInternalConsumer())
                .thenReturn(internalConsumer);
        when(consumerFactory.createConsumer(anyString()))
                .thenReturn(internalConsumer);
        when(internalConsumer.partitionsFor(anyString()))
                .thenReturn(PARTITIONS_INFO);
        when(internalConsumer.beginningOffsets(any(Collection.class)))
                .thenReturn(BEGINNING_OFFSETS);

        source = new KafkaSource<>(
                PARALLELISM,
                new ProtobufType<>(Sensor.class),
                OFFSET_RANGE,
                JOB_ID,
                TOPIC,
                consumerFactory,
                new KafkaProperties.KafkaSourceConfig("1 ms"),
                workerEventProducer
        );

        queues = IntStream.range(0, PARALLELISM)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), i -> new LinkedBlockingQueue<>()));
    }

    @AfterEach
    public void validate() {
        validateMockitoUsage();
        Mockito.reset(consumerFactory, internalConsumer);
    }

    @Test
    void noPartitionsForTopic() {
        when(internalConsumer.partitionsFor(anyString()))
                .thenReturn(new ArrayList<>());

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> source.initialize(queues));
    }

    @Test
    void parallelismHigherThanTotalPartitions() {
        when(internalConsumer.partitionsFor(anyString())).thenReturn(
                IntStream.range(0, PARALLELISM - 1)
                        .mapToObj(i -> new PartitionInfo(TOPIC, i, null, null, null))
                        .toList());

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> source.initialize(queues));
    }

    @Test
    void parallelismLowerTotalPartitions() {
        Collection<Integer> workers = source.initialize(queues);

        assertThat(source.getParallelism()).isEqualTo(PARALLELISM);
        assertThat(workers).containsAll(IntStream.rangeClosed(1, PARALLELISM).boxed().toList());
        verify(consumerFactory, times(1)).createInternalConsumer();
        verify(consumerFactory, times(3)).createConsumer(anyString());
    }

    @Test
    void parallelismSameAsTotalPartitions() {
        Collection<Integer> workers = source.initialize(queues);

        assertThat(source.getParallelism()).isEqualTo(PARALLELISM);
        assertThat(workers).containsAll(IntStream.rangeClosed(1, PARALLELISM).boxed().toList());
        verify(consumerFactory, times(1)).createInternalConsumer();
        verify(consumerFactory, times(3)).createConsumer(anyString());
    }
}
