package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.implementations.kafka.exception.KafkaExceptionHandler;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Producer, which works with only 1 topic, so we don't have to put that into ProducerRecord explicitly every time
 */
public class TopicSpecificKafkaProducer<K, V> implements Producer<K, V> {

    private final Producer<K, V> internalProducer;
    private final String topic;
    private KafkaExceptionHandler<K, V> kafkaExceptionHandler = new KafkaExceptionHandler.AlwaysFailKafkaExceptionHandler<>();

    public TopicSpecificKafkaProducer(Producer<K, V> internalProducer, String topic) {
        this.internalProducer = internalProducer;
        this.topic = topic;
    }

    public TopicSpecificKafkaProducer<K, V> setKafkaExceptionHandler(KafkaExceptionHandler<K, V> kafkaExceptionHandler) {
        this.kafkaExceptionHandler = kafkaExceptionHandler;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public Future<RecordMetadata> send(Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, timestamp, key, value, headers);
        return send(record, new DefaultErrorCallback(record));
    }
    
    public Future<RecordMetadata> send(Integer partition, Long timestamp, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, timestamp, key, value);
        return send(record, new DefaultErrorCallback(record));
    }
    
    public Future<RecordMetadata> send(Integer partition, K key, V value, Iterable<Header> headers) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, key, value, headers);
        return send(record, new DefaultErrorCallback(record));
    }

    public Future<RecordMetadata> send(Integer partition, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, key, value);
        return send(record, new DefaultErrorCallback(record));
    }

    public Future<RecordMetadata> send(K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        return send(record, new DefaultErrorCallback(record));
    }
    
    public Future<RecordMetadata> send(V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, value);
        return send(record, new DefaultErrorCallback(record));
    }

    public List<PartitionInfo> partitions() {
        return internalProducer.partitionsFor(topic);
    }

    @Override
    public void initTransactions() {
        internalProducer.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        internalProducer.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
        internalProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        internalProducer.sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        internalProducer.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        internalProducer.abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return internalProducer.send(
                new ProducerRecord<>(
                        topic,
                        record.partition(),
                        record.timestamp(),
                        record.key(),
                        record.value(),
                        record.headers()
                ));
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return internalProducer.send(
                new ProducerRecord<>(
                        topic,
                        record.partition(),
                        record.timestamp(),
                        record.key(),
                        record.value(),
                        record.headers()
                ), callback);
    }

    @Override
    public void flush() {
        internalProducer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return internalProducer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return internalProducer.metrics();
    }

    @Override
    public void close() {
        internalProducer.close();
    }

    @Override
    public void close(Duration timeout) {
        internalProducer.close(timeout);
    }

    public Producer<K, V> getInternalProducer() {
        return this.internalProducer;
    }

    class DefaultErrorCallback implements Callback {

        private static final Logger log = LoggerFactory.getLogger(org.apache.kafka.clients.producer.internals.ErrorLoggingCallback.class);
        private static final String EXCEPTION_MESSAGE = "Error when sending message to topic %s with record [%s] with error: %n%s";

        private final ProducerRecord<K, V> record;

        DefaultErrorCallback(ProducerRecord<K, V> record) {
            this.record = record;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                boolean fail = false;
                String errorMessage = String.format(EXCEPTION_MESSAGE, topic, record, exception);

                if (!(exception instanceof RetriableException)) {
                    errorMessage += "\nNo more records would be sent since this is a fatal error.";
                    fail = true;
                } else {
                    errorMessage += "\nRetriableException occured. The broker is either slow or in bad state." +
                                    "\nConsider overwriting `max.block.ms` and /or delivery.timeout.ms` to a larger value to wait longer and avoid timeout errors.";

                    if (kafkaExceptionHandler.handle(record, exception) == KafkaExceptionHandler.KafkaExceptionHandlerResponse.FAIL) {
                        errorMessage += "\nException handler choose to FAIL the processing, no more records will be sent.";
                        fail = true;
                    } else {
                        errorMessage += "\nException handler choose to CONTINUE processing in spite of this error.";
                    }
                }

                log.error(errorMessage);
                if (fail) {
                    close();
                    throw new DittoRuntimeException(errorMessage, exception);
                }
            }
        }

    }

}
