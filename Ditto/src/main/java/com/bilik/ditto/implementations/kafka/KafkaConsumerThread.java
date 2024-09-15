package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.util.TimeDateUtils;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent.WorkerState;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.metric.Counter;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.metric.ThreadSafeCounter;
import com.bilik.ditto.core.transfer.Handover;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Commiting is commented out
 */
public class KafkaConsumerThread extends WorkerThread {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerThread.class);

    private final Duration pollTimeout;

    private final Consumer<byte[], byte[]> consumer;

    // committing not needed
//    private volatile AtomicReference<Map<TopicPartition, Long>> offsetsToCommit;
//
//    private volatile boolean commitInProgress;
//
//    private final OffsetCommitCallback offsetCommitCallback = new CommitCallback();

    private final Handover<ConsumerRecords<byte[], byte[]>> handover;

    private final Set<TopicPartition> partitionsToUnassign = new HashSet<>();
    
    private final KafkaRecordRange kafkaRecordRange;

    private final CounterAggregator counterAggregator;

    private final KafkaSplit split;

    private Counter readRecords;
    private Counter emptyPolls;
    private Counter lastPollSize;

    private final Object unassigningLock = new Object();

    public KafkaConsumerThread(WorkerEventProducer eventProducer,
                               Consumer<byte[], byte[]> consumer,
                               Handover<ConsumerRecords<byte[], byte[]>> handover,
                               KafkaSplit split,
                               KafkaRecordRange kafkaRecordRange,
                               CounterAggregator counterAggregator,
                               KafkaProperties.KafkaSourceConfig kafkaSourceConfig) {
        super(eventProducer);
        this.pollTimeout = kafkaSourceConfig.getPollDuration();
        this.consumer = consumer;
        this.handover = handover;
        this.kafkaRecordRange = kafkaRecordRange;
        this.counterAggregator = counterAggregator;
        this.split = split;
//        this.offsetsToCommit = new AtomicReference<>();
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
            produceEvent(WorkerState.STARTED);
        }

        log.info("KafkaConsumerThread is running for TopicPartitions {}", split);

        try {
            // May carry across the loop if the thread is woken up from blocking on the handover
            ConsumerRecords<byte[], byte[]> consumerRecords = null;

            // main loop - loop until interrupted and consumer has work to do
            while (running && !consumer.assignment().isEmpty()) {

                // is fetcher has defined partitions to unassign, do it as first
                if (!partitionsToUnassign.isEmpty()) {
                    int split = internalUnassignPartitions(partitionsToUnassign);
                    partitionsToUnassign.clear();
                    if (split <= 0) {
                        continue;// if there are no partitions, this thread is shutted down, so continue will actualy behave like a break
                    }
                }

                // COMMIT
//                if (!commitInProgress) {
//                    // get and set value to null, so it won't be comitted again
//                    final Map<TopicPartition, Long> offsets = offsetsToCommit.getAndSet(null);
//
//                    if (offsets != null) {
//                        log.debug("Asynchronously commiting offsets.");
//
//                        // commit in progress, so there won't be another one, because there might be issue with overwriting newer commits
//                        commitInProgress = true;
//                        consumer.commitAsync(
//                                offsets.entrySet().stream()
//                                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> new OffsetAndMetadata(entry.getValue()))),
//                                offsetCommitCallback);
//                    }
//                }

                // put to handover did not have to succeed, so we rather check, if record are still there, or we need to poll new ones
                if (consumerRecords == null) {
                    try {
                        consumerRecords = consumer.poll(pollTimeout);
                    } catch (WakeupException ex) {
                        log.debug("Consumer has been woken up while polling.");
                        continue;
                    }

                    if (consumerRecords.isEmpty()) {
                        emptyPolls.inc();
                    }
                    lastPollSize.set(consumerRecords.count());
                    readRecords.inc(consumerRecords.count());
                }


                try {
                    handover.produce(consumerRecords);
                    consumerRecords = null;
                } catch (Handover.WakeupException e) {
                    // fall through the loop
                }

            }
        } catch (Exception ex) {
            log.error("Error occured in KafkaConsumerThread. Sending error over handover to let main thread know about it.");
            ex.printStackTrace();
            handover.reportError(ex);
        } finally {
            // make sure the handover is closed
            handover.closeSoftly();

            try {
                consumer.wakeup();
                consumer.close();
            } catch (Exception ex) {
                log.error("Error while closing Kafka consumer", ex);
            }
        }

        log.info("KafkaConsumerThread is finished");
    }

    /**
     * Finds offsets for every assigned partition
     * lot of logs to exactly know what happened... at least until proven working
     * @return False if thread will have no work to do, otherwise true.
     */
    @Override
    public boolean initThread() {
        super.initThread();
        this.readRecords = counterAggregator.addCounter(getName() + "_read_records", new ThreadSafeCounter());
        this.emptyPolls = counterAggregator.addCounter(getName() + "_empty_polls", new ThreadSafeCounter());
        this.lastPollSize = counterAggregator.addCounter(getName() + "last_poll_size", new ThreadSafeCounter());

        consumer.assign(split.getSplit());
        Collection<TopicPartition> partitionsToUnassign = new ArrayList<>();

        if (kafkaRecordRange.isTimeRange()) {
            Map<TopicPartition, OffsetAndTimestamp> offsetsAndTimestamps =
                    consumer.offsetsForTimes(split.getSplit().stream()
                            .collect(Collectors.toMap(Function.identity(), partition -> kafkaRecordRange.getFrom())));

            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsAndTimestamps.entrySet()) {
                if (entry.getValue() == null) {
                    partitionsToUnassign.add(entry.getKey());
                    log.warn("Partition [{}] does not have message with time equal or greater to {}. This partition will be immediately unassigned",
                            entry.getKey(), Instant.ofEpochMilli(kafkaRecordRange.getFrom()));
                    continue;
                } else if (TimeDateUtils.isSameSecond(entry.getValue().timestamp(), kafkaRecordRange.getFrom())) {
                    log.info("Partition [{}] seeking to position {}, which has time {}",
                            entry.getKey(), entry.getValue().offset(), Instant.ofEpochMilli(entry.getValue().timestamp()));
                } else if (entry.getValue().timestamp() <= kafkaRecordRange.getTo()) {
                    log.info("Partition [{}] has lowest timestamp within our range. Seeking to position {}, which has time {}",
                            entry.getKey(), entry.getValue().offset(), Instant.ofEpochMilli(entry.getValue().timestamp()));
                } else {
                    partitionsToUnassign.add(entry.getKey());
                    log.info("Partition [{}] will be immediately unassigned, because it's lowest offset time [{}] is higher than our range [{}]",
                            entry.getKey(), Instant.ofEpochMilli(entry.getValue().timestamp()), Instant.ofEpochMilli(kafkaRecordRange.getTo()));
                    continue;
                }
                consumer.seek(entry.getKey(), entry.getValue().offset());
            }

        } else {
            log.info("Assigning offsets to consumer for topic {}. Range is {}", split.getTopic(), kafkaRecordRange);

            Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(split.getSplit());

            for (TopicPartition assignedPartition : split.getSplit()) {
                // Maybe wanted offset doesn't exist and closest offset is bigger than our 'To' offset,
                // which would mean, that the partition could be marked as finished and immediately unassigned.
                // So we have to test next polled offset with .position()
                long beginningOffset = beginnings.get(assignedPartition);

                if (beginningOffset <= kafkaRecordRange.getFrom()) {
                    consumer.seek(assignedPartition, kafkaRecordRange.getFrom());
                    log.info("Kafka consumer for [{}] has seeked position {}",
                            assignedPartition, kafkaRecordRange.getFrom());
                } else if (beginningOffset <= kafkaRecordRange.getTo()) {
                    consumer.seek(assignedPartition, beginningOffset);
                    log.info("Kafka consumer for [{}] has seeked position {} [Wanted From = {}], which is not wanted beginning but is lower than range",
                            assignedPartition, beginningOffset, kafkaRecordRange.getFrom());
                } else {
                    // nextPoll is higher than our range, so we can unassign that partition
                    partitionsToUnassign.add(assignedPartition);
                    log.info("Kafka consumer for [{}] will be immediately unassigned, because it's lowest offset [{}] is greater than upper range value [{}]",
                            assignedPartition, beginningOffset, kafkaRecordRange.getTo());
                }
            }
        }

        if (!partitionsToUnassign.isEmpty()) {
            int split = internalUnassignPartitions(partitionsToUnassign);
            if (split <= 0) {
                throw new DittoRuntimeException("There are no timestamps/offsets that would satisfy this job's range requirements");
            }
        }

        log.info("Kafkaconsumer {} has assigned partitions [{}]", getName(), consumer.assignment());
        return true;
    }

//    public void setOffsetsToCommit(Map<TopicPartition, Long> offsetsToCommit) {
//        if (this.offsetsToCommit.getAndSet(offsetsToCommit) != null) {
//            log.warn("Skipping commit of previous offsets because newer offsets are available");
//        }
//    }

    /**
     * TODO - chceme tuto volat shutdown() ? alebo to nehame na fetcher, aby tento thread vobec nebol zodpovedny za zivotny cyklus
     */
    private int internalUnassignPartitions(Collection<TopicPartition> partitionsToUnassign) {
        synchronized (unassigningLock) {
            Collection<TopicPartition> newAssignment = new HashSet<>(consumer. assignment());
            newAssignment.removeAll(partitionsToUnassign);
            consumer.assign(newAssignment);

            log.info("KafkaConsumer has unassigned partitions {}. Current assignmenets [{}]", partitionsToUnassign, newAssignment);

            if (consumer.assignment().isEmpty()) {
                log.info("Kafka Consumer for partitions [{}] has finished.", split);
                shutdown();
            }

            // because we know, that we reached ends on some of the partitions,
            // we can wake up consumer, so he won't poll those records
            consumer.wakeup();
            return consumer.assignment().size();
        }
    }

    /**
     * We can not make real unassigning public, because kafkaconsumer can not be accessed from 2 threads.
     * Instead just fill list of TopicPartitions, which will be unassigned before next poll.
     * Consumer is also woke up
     */
    public void unassignPartitions(Collection<TopicPartition> partitionsToUnassign) {
        synchronized (unassigningLock) {
            if (!this.partitionsToUnassign.isEmpty()) {
                // there are partitions to be unassigned, but we got more fresh one, so rewrite them
                // but we shouldnt get here ever, because we are instantly waking up consumer
                this.partitionsToUnassign.clear();
            }
            this.partitionsToUnassign.addAll(partitionsToUnassign);
            consumer.wakeup();
        }
    }

    /**
     * Shuts this thread down, waking up the thread gracefully if blocked (without
     * Thread.interrupt() calls).
     * This shutdown should be idempotent, because it might be called twice.
     * Once from this thread and once from KafkaWorker.
     */
    @Override
    public void shutdown() {
        super.shutdown();

        // consumer closing is handled in 'finally' block,
        // we just wake him up, so we break out of main while()
        consumer.wakeup();

        // this wakes up the consumer if it is blocked handing over records
        handover.wakeupProducer();
    }


//    private class CommitCallback implements OffsetCommitCallback {
//
//        @Override
//        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) {
//            commitInProgress = false;
//
//            if (ex != null) {
//                log.warn("Commit failed for offsets {}", offsets, ex);
//            } else {
//                String result = offsets.entrySet().stream()
//                        .map(entry -> "[" + topic + " | " + entry.getKey().partition() + ": " +entry.getValue().offset() + "]")
//                        .reduce((first, second) -> first + ", " + second)
//                        .orElse("EMPTY");
//                log.info("Committing offsets to Kafka succeeded. Committed offsets: {}", result);
//            }
//        }
//    }

}
