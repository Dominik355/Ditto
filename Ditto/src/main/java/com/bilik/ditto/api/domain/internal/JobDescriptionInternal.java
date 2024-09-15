package com.bilik.ditto.api.domain.internal;

import com.bilik.ditto.core.type.SupportedPlatform;
import com.bilik.ditto.core.type.Type;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.nio.file.Path;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobDescriptionInternal {

    private int parallelism;
    private String jobId;
    private Path workingPath;

    private SourceGeneral sourceGeneral;
    private SinkGeneral sinkGeneral;

    private Converter converter;

    private KafkaSource kafkaSource;
    private KafkaSink kafkaSink;

    private S3Source s3Source;
    private S3Sink s3Sink;

    private HdfsSource hdfsSource;
    private HdfsSink hdfsSink;

    private LocalSink localSink;

    @Data
    public static class SourceGeneral {
        private final Type<?> dataType;
        private final SupportedPlatform sourceType;
    }

    @Data
    public static class SinkGeneral {
        private final Type<?> dataType;
        private final SupportedPlatform sinkType;
    }

    @Data
    public static class KafkaSource {
        private final long from;
        private final long to;
        private final String rangeType;
        private final String cluster;
        private final String topic;
    }

    @Data
    public static class KafkaSink {
        private final int batchSize;
        private final String cluster;
        private final String topic;
    }

    @Data
    public static class S3Source {
        private final String server;
        private final String bucket;
        private final List<String> objectNamePrefixes;
        private final Long from;
        private final Long to;
    }

    @Data
    public static class S3Sink {
        private final String server;
        private final String bucket;
        private final FileSegmentator fileSegmentator;
    }

    @Data
    public static class LocalSink {
        private final String dir;
        private final FileSegmentator fileSegmentator;
    }

    @Data
    @AllArgsConstructor
    public static class HdfsSource {
        private final String cluster;
        private final String parentPath;
        private final boolean recursive;
        private List<String> prefixes;
        private final Long from;
        private final Long to;
    }

    @Data
    public static class HdfsSink {
        private final String cluster;
        private final String parentPath;
        private final FileSegmentator fileSegmentator;
    }

    @Data
    public static class FileSegmentator {
        private final String maxSizePerFile;
        private final Long maxElementsPerFile;
    }

    @Data
    @ToString
    public static class Converter {
        private final String name;
        private final String specificType;
        private final Object[] args;
    }

}
