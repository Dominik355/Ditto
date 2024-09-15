package com.bilik.ditto.implementations.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class TopicTracker {

    private static final Logger log = LoggerFactory.getLogger(TopicTracker.class);
    
    private static final long DEFAULT_VALUE = -1;

    private final Map<TopicPartition, Holder> lastOffsetMap; // last read offsets
    private final Map<TopicPartition, Holder> firstOffsetMap; // starting offsets
    private final Map<TopicPartition, Holder> lastCommitedOffsetMap; // last commited offsets

    public TopicTracker() {
        lastOffsetMap = new HashMap<>();
        firstOffsetMap = new HashMap<>();
        lastCommitedOffsetMap = new HashMap<>();
    }

    public Holder setStartingOffset(TopicPartition topicPartition, long offset, long timestamp) {
        return firstOffsetMap.putIfAbsent(topicPartition, new Holder(offset, timestamp));
    }

    public long getLastOffset(TopicPartition topicPartition) {
        return getOffset(lastOffsetMap, topicPartition);
    }

    public Map<Integer, Long> getLastOffsets() {
        return lastOffsetMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().partition(),
                        entry -> entry.getValue().offset
                ));
    }

    public long getLastTimestamp(TopicPartition topicPartition) {
        return getTimestamp(lastOffsetMap, topicPartition);
    }

    public long setLastOffset(ConsumerRecord<?, ?> record) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        Holder holder = getOrInit(topicPartition, lastOffsetMap);
        long lastOffset = holder.offset;

        holder.update(record.offset(), record.timestamp());

        if (record.offset() < lastOffset) {
            log.warn("offset for topic {} partition {} goes back from {} to {}",
                    topicPartition.topic(), topicPartition.partition(), lastOffset, record.offset());
        } else if (lastOffset < 0) {
            log.info("starting to consume topic {} partition {} from offset {}, timestamp {}",
                    topicPartition.topic(), topicPartition.partition(), record.offset(), record.timestamp());
        }

        if (firstOffsetMap.get(topicPartition) == null) {
            log.info("Setting starting offset for topic {} partition {} to offset {} with timestamp {}",
                    topicPartition.topic(), topicPartition.partition(), record.offset(), record.timestamp());
            firstOffsetMap.put(topicPartition, new Holder(record.offset(), record.timestamp()));
        }

        return holder.offset;
    }

    public long getCommitedOffset(TopicPartition topicPartition) {
        return getOffset(lastCommitedOffsetMap, topicPartition);
    }

    public long getCommitedTimestamp(TopicPartition topicPartition) {
        return getTimestamp(lastCommitedOffsetMap, topicPartition);
    }

    public Holder setCommittedOffset(TopicPartition topicPartition, long offset, long timestamp) {
        return lastCommitedOffsetMap.replace(topicPartition, new Holder(offset, timestamp));
    }

    public long getOffset(Map<TopicPartition, Holder> map, TopicPartition topicPartition) {
        Holder value = map.get(topicPartition);
        return value == null ? DEFAULT_VALUE : value.offset;
    }

    public long getTimestamp(Map<TopicPartition, Holder> map, TopicPartition topicPartition) {
        Holder value = map.get(topicPartition);
        return value == null ? DEFAULT_VALUE : value.timestamp;
    }

    public Holder getOrInit(TopicPartition topicPartition, Map<TopicPartition, Holder> map) {
        Holder holder = map.get(topicPartition);
        if (holder == null) {
            holder = new Holder(DEFAULT_VALUE, DEFAULT_VALUE);
            map.put(topicPartition, holder);
        }
        return holder;
    }

    private static class Holder {
        private long offset;
        private long timestamp;

        public Holder(long offset, long timestamp) {
            this.offset = offset;
            this.timestamp = timestamp;
        }

        public void update(long offset, long timestmap) {
            this.offset = offset;
            this.timestamp = timestmap;
        }

        public long offset() {
            return offset;
        }

        public long timestamp() {
            return timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Holder holder = (Holder) o;
            return offset == holder.offset && timestamp == holder.timestamp;
        }

        @Override
        public int hashCode() {
            return 31 * Long.hashCode(offset) + Long.hashCode(timestamp);
        }
    }

    public String lastOffsets() {
        StringJoiner joiner = new StringJoiner(" | ", TopicTracker.class.getSimpleName() + "Topic tracket - LAST [", "]");
        lastOffsetMap.forEach((key, value) -> joiner.add(key.partition() + ": " + value.offset + "," + Instant.ofEpochMilli(value.timestamp)));
        return joiner.toString();
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner("\n", TopicTracker.class.getSimpleName() + "TopicTracker [", "]");

        joiner.add("Last offset map:");
        lastOffsetMap.forEach((key, value) -> joiner.add(key.partition() + "-" + value.offset + ", " + value.timestamp));

        joiner.add("First offset map:");
        firstOffsetMap.forEach((key, value) -> joiner.add(key.partition() + "-" + value.offset + ", " + value.timestamp));

        joiner.add("Last committed offset map:");
        lastCommitedOffsetMap.forEach((key, value) -> joiner.add(key.partition() + "-" + value.offset + ", " + value.timestamp));

        return joiner.toString();
    }
}