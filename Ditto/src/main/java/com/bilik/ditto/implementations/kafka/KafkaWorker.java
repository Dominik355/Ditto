package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.metric.Counter;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.metric.ThreadSafeCounter;
import com.bilik.ditto.core.job.CollectingDeserializer.DefaultCollectingDeserializer;
import com.bilik.ditto.core.job.Collector;
import com.bilik.ditto.core.job.CollectingDeserializer;
import com.bilik.ditto.core.transfer.Handover;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.type.TypeSerde;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.implementations.kafka.factory.JobSpecificKafkaConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class KafkaWorker<T> extends WorkerThread {

    private static final Logger log = LoggerFactory.getLogger(KafkaWorker.class);
    
    private final TypeSerde.Deserializer<T> deserializer;

    private final BlockingQueue<StreamElement<T>> sinkQueue;

    private final CounterAggregator counterAggregator;

    private final Handover<ConsumerRecords<byte[], byte[]>> handover;

    private final SuspendPartitionCondition suspendPartitionCondition;

    private KafkaConsumerThread consumerThread;

    private final JobSpecificKafkaConsumerFactory kafkaConsumerFactory;

    private final KafkaSplit split;

    private Map<Integer, Counter> readRecords;

    private final TopicTracker topicTracker;

    private final KafkaRecordRange kafkaRecordRange;

    private final KafkaProperties.KafkaSourceConfig kafkaSourceConfig;

    /** Flag to mark the main work loop as alive. */
    volatile boolean running;

    public KafkaWorker(WorkerEventProducer eventProducer,
                       Type<T> type,
                       BlockingQueue<StreamElement<T>> sinkQueue,
                       CounterAggregator counterAggregator,
                       KafkaSplit split,
                       KafkaRecordRange kafkaRecordRange,
                       JobSpecificKafkaConsumerFactory kafkaConsumerFactory,
                       KafkaProperties.KafkaSourceConfig kafkaSourceConfig) {
        super(eventProducer);
        this.sinkQueue = sinkQueue;
        this.counterAggregator = counterAggregator;
        this.suspendPartitionCondition = new SuspendPartitionCondition.DefaultSuspendPartitionCondition(kafkaRecordRange);
        this.split = split;
        this.kafkaRecordRange = kafkaRecordRange;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.kafkaSourceConfig = kafkaSourceConfig;

        this.topicTracker = new TopicTracker();
        this.handover = new Handover<>();
        this.deserializer = type.createSerde().deserializer();
    }


    @Override
    public boolean initThread() {
        super.initThread();
        this.readRecords = new HashMap<>();
        for(TopicPartition partition : split.getSplit()) {
            this.readRecords.put(partition.partition(), counterAggregator.addCounter(getName() + "_read_records_" + partition.partition(), new ThreadSafeCounter()));
        }
        this.consumerThread = new KafkaConsumerThread(
                workerEventProducer,
                kafkaConsumerFactory.createConsumer(String.valueOf(getWorkerNumber())),
                handover,
                split,
                kafkaRecordRange,
                counterAggregator,
                kafkaSourceConfig
        );
        log.info("Initialized KafkaWorker Thread for splits [{}]", split);

        // because we didn't use factory to create consumer thread, we have to set it this way
        copyProperties(this.consumerThread, "consumer");
        return this.consumerThread.initThread();
    }

    @Override
    public void run() {
        if (running) {
            log.warn("Thread [{}] is already in running state", getName());
            return;
        } else if (!hasBeenInitialized) {
            throw new DittoRuntimeException("WorkerThread has not been initialized yet!");
        } else {
            running = true;
            produceEvent(WorkerEvent.WorkerState.STARTED);
            log.info("KafkaWorker is running");
        }

         try {
             // start actual consumer
             consumerThread.start();

             Collector<T> collector = new Collector.QueueCollector<>(sinkQueue);
             CollectingDeserializer<byte[], T> collectingDeserializer = new DefaultCollectingDeserializer<>(deserializer);

             while (running) {
                 // blocks until we get the next records
                 // it automatically re-throws exceptions encountered in the consumer thread
                 final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
                 Set<TopicPartition> partitionsToUnassign = new HashSet<>();

                 for (TopicPartition tp : split.getSplit()) {

                     for (ConsumerRecord<byte[], byte[]> record : records.records(tp)) {

                         if (!suspendPartitionCondition.test(record)) {
                             partitionsToUnassign.add(new TopicPartition(record.topic(), record.partition()));
                             break;
                         }

                         readRecords.get(record.partition()).inc();
                         topicTracker.setLastOffset(record);
                         collectingDeserializer.deserialize(record.value(), collector, record.timestamp());
                     }

                 }

                 if (!partitionsToUnassign.isEmpty()) {
                     // remove from split, so we won't read that partition from buffered data in handover
                     split.removePartitions(partitionsToUnassign);
                     consumerThread.unassignPartitions(partitionsToUnassign);
                 }

             }
         } catch (Handover.ClosedException ex) {
            log.info("Handover has been closed. It means that the consumer has completed its work");
         } catch (Exception ex) {
             // TODO - chceme produkovat error a zarovne rethrownut exception ?
             produceErrorEvent(ex);
             throw new RuntimeException(ex);
         } finally {
             // this signals the consumer thread that no more work is to be done
             consumerThread.shutdown();
         }

        // on a clean exit, wait for the runner thread
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            // may be the result of a wake-up interruption after an exception.
            // we ignore this here and only restore the interruption state
            interrupt();
        }

        produceEvent(WorkerEvent.WorkerState.FINISHED);
        log.info("KafkaWorker has finished");
    }

    public TopicTracker getTopicTracker() {
        return topicTracker;
    }

    public KafkaSplit getSplit() {
        return split;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        handover.close();
        consumerThread.shutdown();
        interrupt(); // because it might be blocked by waiting to hand over data to collector
    }

}
