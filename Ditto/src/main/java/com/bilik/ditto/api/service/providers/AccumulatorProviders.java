package com.bilik.ditto.api.service.providers;

import com.google.protobuf.Message;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.common.DataSize;
import com.bilik.ditto.core.util.StringUtils;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.FileWriter;
import com.bilik.ditto.core.io.impl.JsonArrayFileRWFactory;
import com.bilik.ditto.core.io.impl.ProtobufParquetFileRWFactory;
import com.bilik.ditto.core.job.output.accumulation.AccumulatorProvider;
import com.bilik.ditto.core.job.output.accumulation.FileAccumulator;
import com.bilik.ditto.core.job.output.accumulation.FileUploader;
import com.bilik.ditto.implementations.kafka.KafkaAccumulator;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.implementations.kafka.TopicSpecificKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.function.Predicate;

public class AccumulatorProviders {

    private static final Logger log = LoggerFactory.getLogger(AccumulatorProviders.class);

    private static final Predicate<FileWriter> ALWAYS_FALSE_PREDICATE = writer -> false;

    public static AccumulatorProvider getProtoParquet(String jobId, Path workingPath, Type<?> dataType, FileUploader fileUploader, JobDescriptionInternal.FileSegmentator fileSegmentator) {
        Predicate<FileWriter> cuttingPredicates =  getFileSegmentatorPredicates(fileSegmentator);
        ProtobufParquetFileRWFactory factory = new ProtobufParquetFileRWFactory(
                FileRWInfo.protoFileInfo(
                        (Class<Message>) dataType.getTypeClass(),
                        workingPath));
        return workerNum -> new FileAccumulator<>(
                factory,
                cuttingPredicates,
                fileUploader,
                jobId,
                workerNum
        );
    }

    public static AccumulatorProvider getJsonArray(String jobId, Path workingPath, FileUploader fileUploader, JobDescriptionInternal.FileSegmentator fileSegmentator) {
        Predicate<FileWriter> cuttingPredicates =  getFileSegmentatorPredicates(fileSegmentator);
        JsonArrayFileRWFactory factory = new JsonArrayFileRWFactory(
                FileRWInfo.jsonFileInfo(workingPath));
        return workerNum -> new FileAccumulator<>(
                factory,
                cuttingPredicates,
                fileUploader,
                jobId,
                workerNum
        );
    }

    public static AccumulatorProvider getKafka(JobDescriptionInternal instruction, TopicSpecificKafkaProducer kafkaProducer) {
        return workerNum -> new KafkaAccumulator<>(
                instruction.getKafkaSink().getBatchSize(),
                kafkaProducer,
                instruction.getSinkGeneral().getDataType().createSerde().serializer()
        );
    }

    /**
     * You can either specify element count or file size.
     * You can also define both, then first condition fulfilled will trigger end of file.
     */
    public static Predicate<FileWriter> getFileSegmentatorPredicates(JobDescriptionInternal.FileSegmentator fileAccumulator) {
        Predicate<FileWriter> predicate = null;

        if (fileAccumulator != null) {
            if (StringUtils.isNotBlank(fileAccumulator.getMaxSizePerFile())) {
                DataSize dataSize = DataSize.fromString(fileAccumulator.getMaxSizePerFile());
                predicate = new FileAccumulator.DataSizeCondition(dataSize);
                log.info("DataSizePredicate created for datasize {}", dataSize);
            }

            if (fileAccumulator.getMaxElementsPerFile() != null) {
                Predicate<FileWriter> predicate2 = new FileAccumulator.ElementCountCondition(fileAccumulator.getMaxElementsPerFile());
                if (predicate != null) {
                    predicate = predicate.or(predicate2);
                } else {
                    predicate = predicate2;
                }
                log.info("MaxElementPredicate created for maxElements {}", fileAccumulator.getMaxElementsPerFile());
            }
        }

        if (predicate == null) {
            log.warn("No FileWriter throttler has been specified. That means that all the data will be written to single file !!!");
            predicate = ALWAYS_FALSE_PREDICATE;
        }

        return predicate;
    }
}
