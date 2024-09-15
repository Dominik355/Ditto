package com.bilik.ditto.api.service.providers;

import com.google.protobuf.Message;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.configuration.properties.HdfsProperties;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.impl.JsonArrayFileRWFactory;
import com.bilik.ditto.core.io.impl.ProtobufParquetFileRWFactory;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import com.bilik.ditto.core.job.output.accumulation.AccumulatorProvider;
import com.bilik.ditto.core.job.output.accumulation.FileUploader;
import com.bilik.ditto.implementations.hdfs.HdfsFileRange;
import com.bilik.ditto.implementations.hdfs.HdfsFileUploader;
import com.bilik.ditto.implementations.hdfs.HdfsSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

@Component
public class HdfsResolver {

    private static final Logger log = LoggerFactory.getLogger(HdfsResolver.class);

    private final Map<String, Configuration> clusters;

    @Autowired
    public HdfsResolver(HdfsProperties hdfsProperties) {
        this.clusters = hdfsProperties.getClusters().stream().
                collect(Collectors.toUnmodifiableMap(
                        HdfsProperties.Cluster::getName,
                        cluster -> {
                            Configuration configuration = new Configuration();
                            configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, cluster.getUri());
                            return configuration;
                        }
                ));
        hdfsProperties.getClusters().forEach(cluster -> log.info("Initialized HDFS instance [name = {}, uri = {}]", cluster.getName(), cluster.getUri()));
    }

    public Collection<String> getAvailableInstances() {
        return clusters.keySet();
    }

    public FileSystem getFileSystem(String cluster) {
        Configuration configuration = clusters.get(cluster);
        if (configuration != null) {
            try {
                return FileSystem.get(configuration);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalArgumentException("HDFS cluster with name " + cluster + " is not registered");
    }

    public Source createSource(JobDescriptionInternal jobDescription, WorkerEventProducer eventProducer) {
        if (jobDescription.getHdfsSource() == null) {
            throw new IllegalArgumentException("HDFS source configuration has not been supplied !");
        }

        log.info("Creating HDFS Source");
        FileSystem fs = getFileSystem(jobDescription.getHdfsSource().getCluster());

        FileRWFactory fileRWFactory = switch (jobDescription.getSourceGeneral().getDataType().dataType()) {
            case PROTOBUF -> new ProtobufParquetFileRWFactory(
                    FileRWInfo.protoFileInfo(
                            (Class<Message>) jobDescription.getSourceGeneral().getDataType().getTypeClass(),
                            jobDescription.getWorkingPath()));
            case JSON -> new JsonArrayFileRWFactory(
                    FileRWInfo.jsonFileInfo(jobDescription.getWorkingPath()));
            default -> throw new IllegalArgumentException("Data type " + jobDescription.getSourceGeneral().getDataType().dataType() + " is not supported for HDFS Source");
        };

        return new HdfsSource.HdfsSourceBuilder()
                .fileRange(HdfsFileRange.from(jobDescription.getHdfsSource()))
                .fileSystem(fs)
                .fileRWFactory(fileRWFactory)
                .jobId(jobDescription.getJobId())
                .eventProducer(eventProducer)
                .parallelism(jobDescription.getParallelism())
                .type(jobDescription.getSourceGeneral().getDataType())
                .build();
    }

    public Sink createSink(JobDescriptionInternal jobDescription, WorkerEventProducer eventProducer, ThreadPoolExecutor executor) {
        if (jobDescription.getHdfsSink() == null) {
            throw new IllegalArgumentException("HDFS sink configuration has not been supplied !");
        }
        log.info("Creating HDFS Sink");

        // every uploader has its own filesystem, so instance is not shared between threads
        FileUploader fileUploader = new HdfsFileUploader(
                new Path(jobDescription.getHdfsSink().getParentPath()),
                getFileSystem(jobDescription.getHdfsSink().getCluster()));

        AccumulatorProvider accumulatorProvider = switch (jobDescription.getSinkGeneral().getDataType().dataType()) {
            case PROTOBUF -> AccumulatorProviders.getProtoParquet(
                    jobDescription.getJobId(),
                    jobDescription.getWorkingPath(),
                    jobDescription.getSinkGeneral().getDataType(),
                    fileUploader,
                    jobDescription.getHdfsSink().getFileSegmentator());
            case JSON -> AccumulatorProviders.getJsonArray(
                    jobDescription.getJobId(),
                    jobDescription.getWorkingPath(),
                    fileUploader,
                    jobDescription.getHdfsSink().getFileSegmentator());
            default -> throw new IllegalArgumentException("Data type " + jobDescription.getSinkGeneral().getDataType().dataType() + " is not supported for HDFS Sink");
        };

        return new Sink(
                jobDescription.getSinkGeneral().getDataType(),
                jobDescription.getJobId(),
                eventProducer,
                executor,
                accumulatorProvider);
    }

}
