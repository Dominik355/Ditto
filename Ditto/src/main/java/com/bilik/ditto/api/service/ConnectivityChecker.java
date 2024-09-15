package com.bilik.ditto.api.service;

import com.bilik.ditto.api.service.providers.HdfsResolver;
import com.bilik.ditto.api.service.providers.KafkaResolver;
import com.bilik.ditto.api.service.providers.S3Resolver;
import com.bilik.ditto.core.configuration.DittoConfiguration;
import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * check is done right in constructor, so application will fail by thrown
 * exception in initialization step of application.
 * You can then use {@link #checkConnectivity} over HTTP API to check, which instances are/not available
 */
@Service
public class ConnectivityChecker {

    private static final Logger log = LoggerFactory.getLogger(ConnectivityChecker.class);

    private final S3Resolver s3Resolver;
    private final KafkaResolver kafkaResolver;
    private final HdfsResolver hdfsResolver;

    @Autowired
    public ConnectivityChecker(S3Resolver s3Resolver,
                               KafkaResolver kafkaResolver,
                               HdfsResolver hdfsResolver,
                               DittoConfiguration dittoConfiguration) {
        this.s3Resolver = s3Resolver;
        this.kafkaResolver = kafkaResolver;
        this.hdfsResolver = hdfsResolver;

        if (dittoConfiguration.isExitOnUnavailableCluster()) {
            for (Map.Entry<String, String> entry : checkS3().entrySet()) {
                if (entry.getValue() != null) {
                    throw DittoRuntimeException.of("Connectivity to S3 instance {} could not be instantiated with message {}", entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<String, String> entry : checkKafka().entrySet()) {
                if (entry.getValue() != null) {
                    throw DittoRuntimeException.of("Connectivity to Kafka cluster {} could not be instantiated with message {}", entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<String, String> entry : checkHdfs().entrySet()) {
                if (entry.getValue() != null) {
                    throw DittoRuntimeException.of("Connectivity to HDFS cluster {} could not be instantiated with message {}", entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * @return Map of platform types [S3, Kafka, HDFS]
     * value is another Map with cluster/instance name as key and optional
     * exception as value which could be thrown by checking connectivity.
     * So null value means, that cluster/instance is available.
     */
    public Map<String, Map<String, String>> checkConnectivity() {
        Map<String, Map<String, String>> clusters = new HashMap<>();
        clusters.put("S3", checkS3());
        clusters.put("Kafka", checkKafka());
        clusters.put("HDFS", checkHdfs());

        return clusters;
    }

    private Map<String, String> checkS3() {
        Map<String, String> instancesAvailability = new HashMap<>();
        for (String instance : s3Resolver.getAvailableInstances()) {
            var handler = s3Resolver.getS3Handler(instance);
            try {
                handler.listBuckets();
            } catch (Exception ex) {
                log.info("Exception occured while checking connectivity of S3 {}", instance);
                instancesAvailability.put(instance, ex.getMessage());
            }
            instancesAvailability.putIfAbsent(instance, null);
            log.info("Checked connectivity for S3 instance {} | {}", instance, instancesAvailability.get(instance));
        }
        return instancesAvailability;
    }

    private Map<String, String> checkKafka() {
        Map<String, String> clustersAvailability = new HashMap<>();
        for (KafkaProperties.Cluster cluster : kafkaResolver.getAvailableClusters()) {
            var adminClient = kafkaResolver.getAdminClient(cluster.getName());
            try {
                adminClient.listTopics();
            } catch (Exception ex) {
                log.info("Exception occured while checking connectivity of Kafka {}", cluster);

                clustersAvailability.put(cluster.getName(), ex.getMessage());
            }
            clustersAvailability.putIfAbsent(cluster.getName(), null);

            log.info("Checked connectivity for Kafka instance {} | {}", cluster, clustersAvailability.get(cluster.getName()));
        }
        return clustersAvailability;
    }

    private Map<String, String> checkHdfs() {
        Map<String, String> clustersAvailability = new HashMap<>();
        for (String cluster : hdfsResolver.getAvailableInstances()) {
            var fs = hdfsResolver.getFileSystem(cluster);
            try {
                fs.exists(new Path("/"));
            } catch (Exception ex) {
                log.info("Exception occured while checking connectivity of HDFS {}", cluster);

                clustersAvailability.put(cluster, ex.getMessage());
            }
            clustersAvailability.putIfAbsent(cluster, null);

            log.info("Checked connectivity for HDFS instance {} | {}", cluster, clustersAvailability.get(cluster));
        }
        return clustersAvailability;
    }

}
