package com.bilik.ditto.api.service.providers;

import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.api.service.KafkaAdminClientService;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import com.bilik.ditto.core.job.output.accumulation.AccumulatorProvider;
import com.bilik.ditto.implementations.kafka.KafkaSource;
import com.bilik.ditto.implementations.kafka.KafkaRecordRange;
import com.bilik.ditto.implementations.kafka.TopicSpecificKafkaProducer;
import com.bilik.ditto.implementations.kafka.factory.JobSpecificKafkaConsumerFactory;
import com.bilik.ditto.implementations.kafka.factory.KafkaConsumerFactory;
import com.bilik.ditto.implementations.kafka.factory.KafkaProducerFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class KafkaResolver {

    private static final Logger log = LoggerFactory.getLogger(KafkaResolver.class);

    private final Map<String, KafkaProperties.Cluster> clusters;
    private final KafkaProperties.KafkaSourceConfig kafkaSourceConfig;

    private final KafkaAdminClientService kafkaAdminClientService;

    @Autowired
    public KafkaResolver(KafkaProperties kafkaProperties,
                         KafkaAdminClientService kafkaAdminClientService) {
        this.kafkaAdminClientService = kafkaAdminClientService;
        this.clusters = kafkaProperties.getClusters().stream().
                collect(Collectors.toUnmodifiableMap(
                        KafkaProperties.Cluster::getName,
                        Function.identity()
                ));
        this.kafkaSourceConfig = kafkaProperties.getKafkaSourceConfig();
        log.info("Initialized kafka properties are : {}", kafkaProperties);
        clusters.values().forEach(cluster -> log.info("Initialized Kafka instance [name = {}, uri = {}]",
                cluster.getName(), cluster.getProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)));
    }

    public Collection<KafkaProperties.Cluster> getAvailableClusters() {
        return clusters.values();
    }

    public KafkaConsumerFactory getConsumerFactory(String clusterName) {
        KafkaProperties.Cluster cluster = clusters.get(clusterName);
        if (cluster != null) {
            return new KafkaConsumerFactory(cluster);
        }
        throw new IllegalArgumentException("Kafka cluster with name " + clusterName + " is not registered");
    }

    public KafkaProducerFactory getProducerFactory(String clusterName) {
        KafkaProperties.Cluster cluster = clusters.get(clusterName);
        if (cluster != null) {
            return new KafkaProducerFactory(cluster);
        }
        throw new IllegalArgumentException("Kafka cluster with name " + clusterName + " is not registered. Available clusters are: " + getAvailableClusters());
    }

    public AdminClient getAdminClient(String clusterName) {
        KafkaProperties.Cluster cluster = clusters.get(clusterName);
        if (cluster != null) {
            return kafkaAdminClientService.get(cluster);
        }
        throw new IllegalArgumentException("Kafka cluster with name " + clusterName + " is not registered. Available clusters are: " + getAvailableClusters());
    }

    /**
     * TODO - this could be in some AdminUtil class - could also handle creation of topics
     */
    public boolean existsTopic(String cluster, String topic) {
        try {
            return getAdminClient(cluster)
                    .listTopics()
                    .names()
                    .get(20, TimeUnit.SECONDS)
                    .contains(topic);
        } catch (Exception e) {
            throw new DittoRuntimeException(e);
        }
    }

    public Sink createSink(JobDescriptionInternal jobDescription, WorkerEventProducer eventProducer, ThreadPoolExecutor executor) {
        if (jobDescription.getKafkaSink() == null) {
            throw new IllegalArgumentException("Kafka sink configuration has not been supplied !");
        }
        if (!existsTopic(jobDescription.getKafkaSink().getCluster(), jobDescription.getKafkaSink().getTopic())) {
            throw new IllegalArgumentException("Kafka topic " + jobDescription.getKafkaSink().getTopic() +
                    " in cluster" + jobDescription.getKafkaSink().getCluster() + " does not exists !");

        }
        log.info("Creating Kafka Sink");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(jobDescription.getKafkaSink().getBatchSize()));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");

        TopicSpecificKafkaProducer kafkaProducer = getProducerFactory(jobDescription.getKafkaSink().getCluster())
                .createKafkaProducer(
                        jobDescription.getJobId(),
                        jobDescription.getKafkaSink().getTopic(),
                        properties);

        AccumulatorProvider accumulatorProvider = AccumulatorProviders.getKafka(jobDescription, kafkaProducer);

        return new Sink(
                jobDescription.getSinkGeneral().getDataType(),
                jobDescription.getJobId(),
                eventProducer,
                executor,
                accumulatorProvider);
    }

    public Source createSource(JobDescriptionInternal jobDescription, WorkerEventProducer eventProducer) {
        if (jobDescription.getKafkaSource() == null) {
            throw new IllegalArgumentException("Kafka source configuration has not been supplied !");
        }
        if (!existsTopic(jobDescription.getKafkaSource().getCluster(), jobDescription.getKafkaSource().getTopic())) {
            throw new IllegalArgumentException("Kafka topic " + jobDescription.getKafkaSource().getTopic() +
                    " in cluster" + jobDescription.getKafkaSource().getCluster() + " does not exists !");

        }
        log.info("Creating Kafka Source");

        JobDescriptionInternal.KafkaSource sourceDescription = jobDescription.getKafkaSource();

        KafkaRecordRange kafkaRecordRange = KafkaRecordRange.range(
                sourceDescription.getFrom(),
                sourceDescription.getTo(),
                KafkaRecordRange.RecordRangeType.fromString(sourceDescription.getRangeType())
                        .orElseThrow(() -> new IllegalArgumentException("Kafka Range type " + sourceDescription.getRangeType() + " does not exist")));

        return new KafkaSource.KafkaSourceBuilder()
                .recordRange(kafkaRecordRange)
                .topic(sourceDescription.getTopic())
                .kafkaConsumerFactory(new JobSpecificKafkaConsumerFactory(
                        getConsumerFactory(sourceDescription.getCluster()),
                        jobDescription.getJobId()))
                .kafkaSourceConfig(kafkaSourceConfig)
                .jobId(jobDescription.getJobId())
                .eventProducer(eventProducer)
                .parallelism(jobDescription.getParallelism())
                .type(jobDescription.getSourceGeneral().getDataType())
                .build();
    }

}
