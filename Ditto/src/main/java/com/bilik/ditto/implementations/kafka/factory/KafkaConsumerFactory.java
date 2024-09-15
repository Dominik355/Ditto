package com.bilik.ditto.implementations.kafka.factory;

import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

/**
 * GroupID pattern: Ditto-{job Id}
 */
public class KafkaConsumerFactory {

    public static final Duration KAFKA_CONNECTIVITY_CHECK_TIMEOUT = Duration.ofSeconds(10);
    public static final Deserializer<byte[]> DEFAULT_DESERIALIZER = new ByteArrayDeserializer();

    private final String clusterName;
    private final Properties properties;

    public KafkaConsumerFactory(KafkaProperties.Cluster cluster) {
        Objects.requireNonNull(cluster, "Cluster configuration can not be null");
        this.properties = cluster.getProperties();
        this.clusterName = cluster.getName();
    }

    public String getClusterName() {
        return clusterName;
    }

    public KafkaConsumer<byte[], byte[]> createConsumer(String groupId, String clientId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        return createConsumer(groupId, props);
    }

    public KafkaConsumer<byte[], byte[]> createConsumer(String groupId, Properties overrideProps) {
        Properties merged = new Properties();
        merged.putAll(properties);
        merged.putAll(overrideProps);
        return createRawConsumer(glueGroupId(groupId), merged);
    }

    /**
     * Used for fetching cluster metadata (no reading/writing)
     */
    public KafkaConsumer<byte[], byte[]> createInternalConsumer() {
        return createRawConsumer("Ditto", properties);
    }

    protected KafkaConsumer<byte[], byte[]> createRawConsumer(String groupId, Properties props) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
        KafkaConsumer<byte[], byte[]> consumer;
        try {
            consumer = new KafkaConsumer<>(props, DEFAULT_DESERIALIZER, DEFAULT_DESERIALIZER);
        } catch (Exception ex) {
            throw new DittoRuntimeException("Creation of KafkaConsumer failed. Check given boostrap url and your VPN", ex);
        }

        // connectivity check
        try {
            consumer.listTopics(KAFKA_CONNECTIVITY_CHECK_TIMEOUT);
        } catch (Exception ex) {
            throw DittoRuntimeException.of(ex,
                    "Could not establish connection for consumer [{}] under {}. Servers : {}", groupId, KAFKA_CONNECTIVITY_CHECK_TIMEOUT, props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        }

        return consumer;
    }

    private String glueGroupId(String name) {
        return "Ditto-" + name;
    }

}
