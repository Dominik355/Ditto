package com.bilik.ditto.api.service.providers;

import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.configuration.DittoConfiguration;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import com.bilik.ditto.core.job.output.accumulation.AccumulatorProvider;
import com.bilik.ditto.implementations.local.LocalFileUploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.ThreadPoolExecutor;

@Component
public class LocalFsResolver {

    private static final Logger log = LoggerFactory.getLogger(LocalFsResolver.class);

    private final boolean isLocalFsAllowed;

    @Autowired
    public LocalFsResolver(DittoConfiguration dittoConfiguration) {
        this.isLocalFsAllowed = dittoConfiguration.isLocalInstance();
    }

    public Source createSource(JobDescriptionInternal jobDescription, WorkerEventProducer eventProducer) {
        throw new UnsupportedOperationException("LocalFS source is not supported");
    }

    public Sink createSink(JobDescriptionInternal jobDescription, WorkerEventProducer eventProducer, ThreadPoolExecutor executor) {
        requireLocalAllowed();
        if (jobDescription.getLocalSink() == null) {
            throw new IllegalArgumentException("LocalFS sink configuration has not been supplied !");
        }

        log.info("Creating LocalFS Sink");

        LocalFileUploader fileUploader = new LocalFileUploader(Path.of(jobDescription.getLocalSink().getDir()));

        AccumulatorProvider accumulatorProvider = switch (jobDescription.getSinkGeneral().getDataType().dataType()) {
            case PROTOBUF -> AccumulatorProviders.getProtoParquet(
                    jobDescription.getJobId(),
                    jobDescription.getWorkingPath(),
                    jobDescription.getSinkGeneral().getDataType(),
                    fileUploader,
                    jobDescription.getLocalSink().getFileSegmentator());
            case JSON -> AccumulatorProviders.getJsonArray(
                    jobDescription.getJobId(),
                    jobDescription.getWorkingPath(),
                    fileUploader,
                    jobDescription.getLocalSink().getFileSegmentator());
            default -> throw new IllegalArgumentException("Data type " + jobDescription.getSinkGeneral().getDataType().dataType() + " is not supported for LocalFC Sink");
        };

        return new Sink(
                jobDescription.getSinkGeneral().getDataType(),
                jobDescription.getJobId(),
                eventProducer,
                executor,
                accumulatorProvider);
    }

    /**
     * Throws an exception if this ditto instance is not running as local
     */
    private void requireLocalAllowed() {
        if (!isLocalFsAllowed) {
            throw new IllegalStateException("Your Ditto instance is not running as local. You can not use LocalFS!");
        }
    }

}
