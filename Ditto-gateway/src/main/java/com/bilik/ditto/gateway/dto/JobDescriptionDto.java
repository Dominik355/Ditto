package com.bilik.ditto.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobDescriptionDto {

    private boolean shallBeQueued;
    private int parallelism;

    private SourceGeneral sourceGeneral;
    private SinkGeneral sinkGeneral;

    private SpecialConverter specialConverter;

    private KafkaSource kafkaSource;
    private KafkaSink kafkaSink;

    private S3Source s3Source;
    private S3Sink s3Sink;

    private HdfsSource hdfsSource;
    private HdfsSink hdfsSink;

    private LocalSink localSink;

    public JobDescriptionDto() {}

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

    public JobDescriptionDto(boolean shallBeQueued,
                             int parallelism,
                             SourceGeneral sourceGeneral,
                             SinkGeneral sinkGeneral,
                             SpecialConverter specialConverter,
                             KafkaSource kafkaSource,
                             KafkaSink kafkaSink,
                             S3Source s3Source,
                             S3Sink s3Sink,
                             HdfsSource hdfsSource,
                             HdfsSink hdfsSink,
                             LocalSink localSink) {
        this.shallBeQueued = shallBeQueued;
        this.parallelism = parallelism;
        this.sourceGeneral = sourceGeneral;
        this.sinkGeneral = sinkGeneral;
        this.specialConverter = specialConverter;
        this.kafkaSource = kafkaSource;
        this.kafkaSink = kafkaSink;
        this.s3Source = s3Source;
        this.s3Sink = s3Sink;
        this.hdfsSource = hdfsSource;
        this.hdfsSink = hdfsSink;
        this.localSink = localSink;
    }

    public boolean isShallBeQueued() {
        return shallBeQueued;
    }

    public int getParallelism() {
        return parallelism;
    }

    public SourceGeneral getSourceGeneral() {
        return sourceGeneral;
    }

    public SinkGeneral getSinkGeneral() {
        return sinkGeneral;
    }

    public SpecialConverter getSpecialConverter() {
        return specialConverter;
    }

    public KafkaSource getKafkaSource() {
        return kafkaSource;
    }

    public KafkaSink getKafkaSink() {
        return kafkaSink;
    }

    public S3Source getS3Source() {
        return s3Source;
    }

    public S3Sink getS3Sink() {
        return s3Sink;
    }

    public HdfsSource getHdfsSource() {
        return hdfsSource;
    }

    public HdfsSink getHdfsSink() {
        return hdfsSink;
    }

    public LocalSink getLocalSink() {
        return localSink;
    }
}
