package com.bilik.ditto.implementations.kafka.factory;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * just stores JobId to create client.id more comfortably
 */
public class JobSpecificKafkaConsumerFactory {

    private final KafkaConsumerFactory consumerFactory;
    private final String jobId;


    public JobSpecificKafkaConsumerFactory(KafkaConsumerFactory consumerFactory, String jobId) {
        this.consumerFactory = consumerFactory;
        this.jobId = jobId;
    }

    public Consumer<byte[], byte[]> createConsumer(String clientId) {
        return consumerFactory.createConsumer(jobId, clientId);
    }

    public Consumer<byte[], byte[]> createInternalConsumer() {
        return consumerFactory.createInternalConsumer();
    }
}
