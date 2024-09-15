package com.bilik.ditto.implementations.kafka;

import com.bilik.ditto.core.transfer.SourceSplit;
import com.bilik.ditto.core.util.Preconditions;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class KafkaSplit implements SourceSplit<List<TopicPartition>> {

    private final List<TopicPartition> partitions;

    public KafkaSplit(List<TopicPartition> partitions) {
        Preconditions.requireNotEmpty(partitions);
        this.partitions = new ArrayList<>(partitions);
    }

    public static KafkaSplit fromPartitionInfos(List<PartitionInfo> partitionInfos) {
        return new KafkaSplit(partitionInfos.stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList()));
    }

    @Override
    public List<TopicPartition> getSplit() {
        return Collections.unmodifiableList(partitions);
    }

    @Override
    public int size() {
        return partitions.size();
    }

    public void removePartition(int partitionNum) {
        partitions.stream()
                .filter(partition -> partition.partition() == partitionNum)
                .findFirst()
                .ifPresent(partitions::remove);
    }

    public void removePartitions(Collection<TopicPartition> toRemove) {
        partitions.removeAll(toRemove);
    }

    public String getTopic() {
        return partitions.get(0).topic();
    }

    @Override
    public String toString() {
        return String.format("KafkaSplit[partitions: %s]", partitions);
    }

}
