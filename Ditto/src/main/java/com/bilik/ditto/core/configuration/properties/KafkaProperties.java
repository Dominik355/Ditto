package com.bilik.ditto.core.configuration.properties;

import com.bilik.ditto.core.common.DurationResolver;
import com.bilik.ditto.core.configuration.ResourceUtils;
import com.bilik.ditto.core.exception.InvalidUserInputException;
import com.bilik.ditto.core.configuration.validation.ValidDuration;
import com.bilik.ditto.core.util.FileUtils;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang.StringUtils.isBlank;

@ConfigurationProperties("ditto.kafka")
@Data
public class KafkaProperties {

    private static final Logger log = LoggerFactory.getLogger(KafkaProperties.class);

    private List<Cluster> clusters = new ArrayList<>();
    @NotNull
    private KafkaSourceConfig kafkaSourceConfig;
    private String directory;

    @Data
    public static class KafkaSourceConfig {
        @NotBlank
        @ValidDuration
        private final String pollDuration;

        public Duration getPollDuration() {
            return DurationResolver.resolveDuration(pollDuration);
        }
    }

    @Data
    @AllArgsConstructor
    public static class Cluster {
        @Setter(AccessLevel.NONE)
        private final String name;
        private Properties properties;

        public void addProperties(Properties properties) {
            this.properties.putAll(properties);
        }
    }

    @PostConstruct
    public void validate() throws IOException {
        addClustersFromResourceFiles();
        addClustersFromSystemFiles();
        validateClusters();
        fillDefault();
    }

    private void addClustersFromResourceFiles() throws IOException {
        List<? extends Resource> resourceList = ResourceUtils.listClassPathResources("kafkaRegistry/");
        log.info("Loaded these classpath resources: {}", resourceList);
        resolveRecources(resourceList);
    }

    private void addClustersFromSystemFiles() throws IOException {
        if (directory == null || directory.isEmpty()) {
            log.info("Filesystem kafka properties has not been specified");
        } else {
            Path dirPath = Paths.get(directory);
            log.info("Loading filesystem resources for kafka from directory {}", dirPath);
            List<? extends Resource> resourceList = FileUtils.listFiles(dirPath).stream()
                    .map(path -> new FileSystemResource(path.toFile()))
                    .toList();
            log.info("Loaded these filesystem resources for kafka: {}", resourceList);
            resolveRecources(resourceList);
        }
    }

    private void resolveRecources(List<? extends Resource> resources) throws IOException {
        for (Resource resource : resources) {
            String name = resource.getFilename().replace(".properties", "");
            checkForDuplicates(name);

            Map<String, Object> mapProperties = ResourceUtils.loadPropertiesAsMap(resource)
                    .entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (v1, v2) -> {throw new InvalidUserInputException("Same keys were defined for [" + name + "]KafkaCluster's property " + v1);}));

            Properties properties = new Properties();
            properties.putAll(mapProperties);

            clusters.stream()
                    .filter(cluster -> cluster.getName().equals(name))
                    .findFirst()
                    .ifPresentOrElse(
                            cluster -> cluster.addProperties(properties),
                            () ->  clusters.add(new Cluster(name, properties)));
            log.info("Loaded Kafka Cluster from file properties [name = {}, properties = {}]", name, properties);
        }
    }

    private void validateClusters() {
        Set<String> clusterNames = new HashSet<>();
        for (Cluster clusterProperties : clusters) {
            if (isBlank(clusterProperties.getName())) {
                throw new InvalidUserInputException(
                        "Configuration isn't valid. Cluster names have to be provided");
            }
            if (!clusterNames.add(clusterProperties.getName())) {
                throw new InvalidUserInputException(
                        "Configuration isn't valid. Two clusters can't have the same name: " + clusterProperties.getName());
            }
            if (clusterProperties.getProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) == null) {
                throw new InvalidUserInputException(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + " property has to be specified for Kafka Cluster " + clusterProperties.getName());
            }
        }
    }

    private void fillDefault() {
        for (Cluster clusterProperties : clusters) {
            clusterProperties.getProperties().putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
            clusterProperties.getProperties().putIfAbsent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10");
        }
    }

    private void checkForDuplicates(String kafkacluster) {
        clusters.stream()
                .filter(cluster -> cluster.getName().equals(kafkacluster))
                .findFirst()
                .ifPresent(name -> {throw new InvalidUserInputException("");});
    }

}
