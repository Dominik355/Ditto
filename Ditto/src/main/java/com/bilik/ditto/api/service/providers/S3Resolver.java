package com.bilik.ditto.api.service.providers;

import com.google.protobuf.Message;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.configuration.properties.S3Properties;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.impl.JsonArrayFileRWFactory;
import com.bilik.ditto.core.io.impl.ProtobufParquetFileRWFactory;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import com.bilik.ditto.core.job.output.accumulation.AccumulatorProvider;
import com.bilik.ditto.implementations.s3.S3Handler;
import com.bilik.ditto.implementations.s3.S3ObjectRange;
import com.bilik.ditto.implementations.s3.S3Source;
import com.bilik.ditto.implementations.s3.S3Uploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class S3Resolver {

    private static final Logger log = LoggerFactory.getLogger(S3Resolver.class);

    private final Map<String, S3Properties.Instance> instances;

    @Autowired
    public S3Resolver(S3Properties s3Properties) {
        this.instances = s3Properties.getInstances().stream().
                collect(Collectors.toUnmodifiableMap(
                        S3Properties.Instance::getName,
                        Function.identity()
                ));
        instances.values().forEach(instance -> log.info("Initialized S3 instance [name = {}, uri = {}]", instance.getName(), instance.getURI()));
    }

    public Collection<String> getAvailableInstances() {
        return instances.keySet();
    }

    public S3Handler getS3Handler(String instanceName) {
        S3Properties.Instance instance = instances.get(instanceName);
        if (instance != null) {
            return new S3Handler(instance.getProperties(), instance.getURI());
        }
        throw new IllegalArgumentException("S3 server with name " + instanceName + " is not registered");
    }

    public Source createSource(JobDescriptionInternal jobDescription, WorkerEventProducer eventProducer) {
        if (jobDescription.getS3Source() == null) {
            throw new IllegalArgumentException("S3 source configuration has not been supplied !");
        }

        S3Handler s3Handler = getS3Handler(jobDescription.getS3Source().getServer());

        if (!s3Handler.exists(jobDescription.getS3Source().getBucket())) {
            throw new IllegalArgumentException("S3 bucket " + jobDescription.getS3Source().getBucket() +
                    " at instance" + jobDescription.getS3Source().getServer() + " does not exists !");

        }

        log.info("Creating S3 Source");

        FileRWFactory fileRWFactory = switch (jobDescription.getSourceGeneral().getDataType().dataType()) {
            case PROTOBUF -> new ProtobufParquetFileRWFactory(
                    FileRWInfo.protoFileInfo(
                            (Class<Message>) jobDescription.getSourceGeneral().getDataType().getTypeClass(),
                            jobDescription.getWorkingPath()));
            case JSON -> new JsonArrayFileRWFactory(
                    FileRWInfo.jsonFileInfo(jobDescription.getWorkingPath()));
            default -> throw new IllegalArgumentException("Data type " + jobDescription.getSourceGeneral().getDataType().dataType() + " is not supported for S3 Source");
        };

        return new S3Source.S3SourceBuilder()
                .s3ObjectRange(S3ObjectRange.from(jobDescription.getS3Source()))
                .bucket(jobDescription.getS3Source().getBucket())
                .s3Handler(s3Handler)
                .fileRWFactory(fileRWFactory)
                .eventProducer(eventProducer)
                .parallelism(jobDescription.getParallelism())
                .type(jobDescription.getSourceGeneral().getDataType())
                .jobId(jobDescription.getJobId())
                .build();
    }

    public Sink createSink(JobDescriptionInternal jobDescription, WorkerEventProducer eventProducer, ThreadPoolExecutor executor) {
        if (jobDescription.getS3Sink() == null) {
            throw new IllegalArgumentException("S3 sink configuration has not been supplied !");
        }

        S3Handler s3Handler = getS3Handler(jobDescription.getS3Sink().getServer());

        if (!s3Handler.exists(jobDescription.getS3Sink().getBucket())) {
            throw new IllegalArgumentException("S3 bucket " + jobDescription.getS3Sink().getBucket() +
                    " at instance" + jobDescription.getS3Sink().getServer() + " does not exists !");

        }

        log.info("Creating S3 Sink");
        S3Uploader.FileS3Uploader fileUploader = S3Uploader.fileUploader(
                s3Handler,
                jobDescription.getJobId(),
                jobDescription.getS3Sink().getBucket());

        AccumulatorProvider accumulatorProvider = switch (jobDescription.getSinkGeneral().getDataType().dataType()) {
            case PROTOBUF -> AccumulatorProviders.getProtoParquet(
                    jobDescription.getJobId(),
                    jobDescription.getWorkingPath(),
                    jobDescription.getSinkGeneral().getDataType(),
                    fileUploader,
                    jobDescription.getS3Sink().getFileSegmentator());
            case JSON -> AccumulatorProviders.getJsonArray(
                    jobDescription.getJobId(),
                    jobDescription.getWorkingPath(),
                    fileUploader,
                    jobDescription.getS3Sink().getFileSegmentator());
            default -> throw new IllegalArgumentException("Data type " + jobDescription.getSinkGeneral().getDataType().dataType() + " is not supported for S3 Sink");
        };

        return new Sink(
                jobDescription.getSinkGeneral().getDataType(),
                jobDescription.getJobId(),
                eventProducer,
                executor,
                accumulatorProvider);
    }

}
