package com.bilik.ditto.api.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class JobDescriptionDto {

    private final boolean shallBeQueued;
    private final int parallelism;

    private final SourceGeneral sourceGeneral;
    private final SinkGeneral sinkGeneral;

    private final SpecialConverter specialConverter;

    private final KafkaSource kafkaSource;
    private final KafkaSink kafkaSink;

    private final S3Source s3Source;
    private final S3Sink s3Sink;

    private final HdfsSource hdfsSource;
    private final HdfsSink hdfsSink;

    private final LocalSink localSink;

    public record SourceGeneral(DataType dataType,
                                String platform) {}

    public record SinkGeneral(DataType dataType,
                              String platform) {}

    public record KafkaSource(long from,
                              long to, 
                              String rangeType, 
                              String cluster, 
                              String topic) {}

    public record KafkaSink(int batchSize, 
                            String cluster,
                            String topic) {}

    public record S3Source(String server, 
                           String bucket,
                           List<String> objectNamePrefixes,
                           Long from,
                           Long to) {}

    public record S3Sink(String server, 
                         String bucket,
                         FileSegmentator fileSegmentator) {}

    public record LocalSink(String dir, 
                            FileSegmentator fileSegmentator) {}

    public record HdfsSource(String cluster, 
                             String parentPath,
                             boolean recursive,
                             List<String> prefixes,
                             Long from,
                             Long to) {}
 
    public record HdfsSink(String cluster, 
                              String parentPath, 
                              FileSegmentator fileSegmentator) {}

    public record DataType(String type, 
                           String specificType) {}

    public record FileSegmentator(String maxSizePerFile,
                                  Long maxElementsPerFile) {}

    public record SpecialConverter(String name,
                                   String specificType,
                                   Object[] args) {}
}
