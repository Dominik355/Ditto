package com.bilik.ditto.implementations.kafka.factory;

import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.implementations.kafka.TopicSpecificKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Objects;
import java.util.Properties;

/**
 * Because KafkaProducer is used only in job and every job works with only 1 topic,
 * TopicSpecificKafkaProducer wrapper class has been created, so we reduce mistakes
 * GroupID pattern: Ditto-{job Id}
 */
public class KafkaProducerFactory {

    public static final Serializer<byte[]> DEFAULT_SERIALIZER = new ByteArraySerializer();

    private final String clusterName;
    private final Properties properties;

    public KafkaProducerFactory(KafkaProperties.Cluster cluster) {
        Objects.requireNonNull(cluster, "Cluster configuration can not be null");
        this.clusterName = cluster.getName();
        this.properties = cluster.getProperties();
    }

    public String getClusterName() {
        return clusterName;
    }

    public TopicSpecificKafkaProducer<byte[], byte[]> createKafkaProducer(String jobId, String topic) {
        return createRawKafkaProducer(glueGroupId(jobId), properties, topic);
    }

    public TopicSpecificKafkaProducer<byte[], byte[]> createKafkaProducer(String jobId, String topic, Properties overrideProps) {
        Properties merged = new Properties();
        merged.putAll(properties);
        merged.putAll(overrideProps);
        return createRawKafkaProducer(glueGroupId(jobId), merged, topic);
    }

    protected TopicSpecificKafkaProducer<byte[], byte[]> createRawKafkaProducer(String groupId, Properties props, String topic) {
        Objects.requireNonNull(topic, "Topic for KafkaProducer can not be null");

        props.put(ProducerConfig.CLIENT_ID_CONFIG, groupId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new TopicSpecificKafkaProducer<>(new KafkaProducer<>(props, DEFAULT_SERIALIZER, DEFAULT_SERIALIZER), topic);
    }

    private String glueGroupId(String name) {
        return "Ditto-" + name;
    }

}
