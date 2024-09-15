package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.common.collections.Tuple2;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.output.accumulation.SinkAccumulator;
import com.bilik.ditto.core.type.TypeSerde;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.util.Preconditions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Yea, we don't really use keys here...
 */
public class KafkaAccumulator<IN> implements SinkAccumulator<IN> {

    private static final Logger log = LoggerFactory.getLogger(KafkaAccumulator.class);

    private final RoundRobinPartitioner partitioner;
    private final int maxElementsPerBatch;
    private final TopicSpecificKafkaProducer<byte[], byte[]> producer;
    private final TypeSerde.Serializer<IN> serializer;

    private final Collection<Tuple2<byte[], Long>> elements; // cleared and re-used, so can be final

    public KafkaAccumulator(int maxElementsPerBatch,
                            TopicSpecificKafkaProducer<byte[], byte[]> producer,
                            TypeSerde.Serializer<IN> serializer) {
        this.maxElementsPerBatch = Preconditions.requireBiggerThan(maxElementsPerBatch, 0);
        this.producer = producer;
        this.partitioner = new RoundRobinPartitioner(producer.partitions());
        this.elements = new ArrayList<>(maxElementsPerBatch);
        this.serializer = serializer;
    }


    @Override
    public void collect(StreamElement<IN> element) throws InterruptedException, IOException {
        elements.add(Tuple2.of(serializer.serialize(
                element.value()),
                element.hasTimestamp() ? element.timestamp() : null));
    }

    @Override
    public boolean isFull() {
        return elements.size() >= maxElementsPerBatch;
    }

    @Override
    public Runnable onFull() throws Exception {
        List<ProducerRecord<byte[], byte[]>> records = partitioner.createProducerRecords(elements);
        this.elements.clear();

        return () -> {
            log.debug("Sending {} records to [{} | {}]", records.size(), records.get(0).topic(), records.get(0).partition());
            records.forEach(record -> producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    throw new DittoRuntimeException("Error occured when sending data to kafka. Problematic record: " + record, exception);
                }
            }));
        };
    }

    @Override
    public Runnable onFinish() throws Exception {
        return onFull();
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    /**
     * RoundRobinPartitioner works in terms of batches not individual elements.
     * We want to batch-send kafka records to increase throughput.
     * Producer just takes 1 element per send, but there is a way to batch-send.
     * <p>We have to :
     * <p>1. send all record to same partition,
     * <p>2. use configs : 'batch.size', 'linger.ms'
     * <p>batch.size should be same as maxElementsPerBatch, so send will fire immediately after every batch
     */
    public static class RoundRobinPartitioner {

        private int current;
        private final int[] partitions;
        private final String topic;

        public RoundRobinPartitioner(Collection<PartitionInfo> partitions) {
            this.topic = partitions.iterator().next().topic();
            this.partitions = new int[partitions.size()];

            Iterator<PartitionInfo> iterator = partitions.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                this.partitions[i++] = iterator.next().partition();
            }
        }

        public List<ProducerRecord<byte[], byte[]>> createProducerRecords(Collection<Tuple2<byte[], Long>> elements) {
            List<ProducerRecord<byte[], byte[]>> records = elements.stream()
                    .map(element -> new ProducerRecord<byte[], byte[]>(topic, partitions[current], element.getT2(), null, element.getT1()))
                    .collect(Collectors.toList());

            shiftCurrent();
            return records;
        }

        private void shiftCurrent() {
            current = (current + 1) % partitions.length;
        }

    }
}
