package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.core.common.StoppingUncaughtExceptionHandler;
import com.bilik.ditto.core.util.CollectionUtils;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.WorkerThreadFactory;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.implementations.kafka.factory.JobSpecificKafkaConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

public class KafkaSource<T> extends Source<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

    private final KafkaRecordRange kafkaRecordRange;

    private final String topic;

    private final JobSpecificKafkaConsumerFactory kafkaConsumerFactory;

    private final KafkaProperties.KafkaSourceConfig kafkaSourceConfig;

    public KafkaSource(int parallelism,
                       Type<T> type,
                       KafkaRecordRange kafkaRecordRange,
                       String jobId,
                       String topic,
                       JobSpecificKafkaConsumerFactory kafkaConsumerFactory,
                       KafkaProperties.KafkaSourceConfig kafkaSourceConfig,
                       WorkerEventProducer eventProducer) {
        super(parallelism, type, jobId, eventProducer);
        this.kafkaRecordRange = kafkaRecordRange;
        this.topic = topic;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.kafkaSourceConfig = kafkaSourceConfig;
    }

    /**
     * supposed to be called before start()
     */
    @Override
    public Collection<Integer> initialize(Map<Integer, BlockingQueue<StreamElement<T>>> workerMap) {
        WorkerThreadFactory<KafkaWorker<T>> threadFactory =
                new WorkerThreadFactory<>(jobId,
                        LoggingUncaughtExceptionHandler.withChild(
                                new StoppingUncaughtExceptionHandler(this)),
                        WorkerThread.WorkerType.SOURCE);

        workerThreads = new HashMap<>();

        // obtain partition for topic
        List<PartitionInfo> partitionsinfo;
        try (Consumer<byte[], byte[]> consumer = kafkaConsumerFactory.createInternalConsumer()) {
            partitionsinfo = consumer.partitionsFor(topic);
        }

        // checks
        if (partitionsinfo.isEmpty()) {
            throw new IllegalStateException("Topic " + topic + " has not been found");
        } else if (partitionsinfo.size() < parallelism) {
            throw new IllegalStateException("Source's parallelism[" + parallelism + "] can not be higher than total partitions[" + partitionsinfo.size() + "] for topic " + topic);
        }

        List<List<PartitionInfo>> splitted = CollectionUtils.toNParts(partitionsinfo, parallelism);

        List<KafkaSplit> kafkaSplits = splitted.stream()
                .map(KafkaSplit::fromPartitionInfos)
                .collect(Collectors.toList());

        if (workerMap.size() != parallelism || parallelism != kafkaSplits.size()) {
            log.error("Sizes does not correspond. workerMap {}, parallelism {}, kafkaSplits {}", workerMap.size(), parallelism, kafkaSplits.size());
        }

        for (int split = 1; split <= parallelism; split++) {
            log.info("Initializing kafka consumer {}/{}", split, parallelism);
            KafkaWorker<T> workerThread = threadFactory.modifyThread(new KafkaWorker<>(
                    eventProducer,
                    type,
                    workerMap.get(split),
                    counterAggregator,
                    kafkaSplits.get(split - 1),
                    kafkaRecordRange,
                    kafkaConsumerFactory,
                    kafkaSourceConfig
            ), split);

            boolean initialized = workerThread.initThread();
            if(!initialized) {
                continue;
            }

            workerThreads.put(workerThread.getWorkerNumber(), workerThread);
        }

        this.parallelism = workerThreads.size();
        return workerThreads.keySet();
    }

    public Map<Integer, Long> currentOffsets() {
        Map<Integer, Long> offsets = new HashMap<>();
        for (WorkerThread wt : workerThreads.values()) {
            KafkaWorker<T> kw = (KafkaWorker<T>) wt;
            offsets.putAll(kw.getTopicTracker().getLastOffsets());
        }
        return offsets;
    }

    // BUILDER
    public static class KafkaSourceBuilder<T> extends Source.SourceBuilder<T, KafkaSourceBuilder<T>> {
        private KafkaRecordRange kafkaRecordRange;
        private String topic;
        private JobSpecificKafkaConsumerFactory kafkaConsumerFactory;
        private KafkaProperties.KafkaSourceConfig kafkaSourceConfig;

        public KafkaSourceBuilder<T> recordRange(KafkaRecordRange kafkaRecordRange) {
            this.kafkaRecordRange = kafkaRecordRange;
            return this;
        }

        public KafkaSourceBuilder<T> topic(String topic) {
            this.topic = topic;
            return this;
        }

        public KafkaSourceBuilder<T> kafkaConsumerFactory(JobSpecificKafkaConsumerFactory kafkaConsumerFactory) {
            this.kafkaConsumerFactory = kafkaConsumerFactory;
            return this;
        }

        public KafkaSourceBuilder<T> kafkaSourceConfig(KafkaProperties.KafkaSourceConfig kafkaSourceConfig) {
            this.kafkaSourceConfig = kafkaSourceConfig;
            return this;
        }

        @Override
        public KafkaSource<T> build() {
            return new KafkaSource<>(parallelism, type, kafkaRecordRange, jobId, topic, kafkaConsumerFactory, kafkaSourceConfig, eventProducer);

        }

        @Override
        protected KafkaSourceBuilder<T> self() {
            return this;
        }
    }

}
