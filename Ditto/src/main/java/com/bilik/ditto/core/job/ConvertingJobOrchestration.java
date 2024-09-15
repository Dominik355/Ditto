package com.bilik.ditto.core.job;

import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.core.common.StoppingUncaughtExceptionHandler;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.WorkerThreadFactory;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventCommunicator;
import com.bilik.ditto.core.configuration.DittoConfiguration;
import com.bilik.ditto.core.convertion.Converter;
import com.bilik.ditto.core.convertion.ConverterWorker;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConvertingJobOrchestration<SRC, SNK> extends JobOrchestration<SRC, SNK> {

    private static final Logger log = LoggerFactory.getLogger(ConvertingJobOrchestration.class);

    private final Supplier<Converter> converterSupplier;

    public ConvertingJobOrchestration(
                            Source<SRC> source,
                            Sink<SNK> sink,
                            DittoConfiguration.OrchestrationConfig configuration,
                            WorkerEventCommunicator workerEventCommunicator,
                            String jobId,
                            int parallelism,
                            Consumer<JobOrchestration<?,?>> whenFinished,
                            Supplier<Converter> converterSupplier) {
        super(source, sink, configuration, workerEventCommunicator, jobId, parallelism, whenFinished);
        this.converterSupplier = Objects.requireNonNull(converterSupplier, "Converter can not be null for ConvertingJobOrchestration");
    }

    @Override
    protected void initializeQueues() {
        this.sourceQueues = IntStream.rangeClosed(1, parallelism)
                .boxed()
                .collect(Collectors.toMap(
                        Function.identity(),
                        i -> new LinkedBlockingQueue<>(getQueueSize(true))
                ));

        this.sinkQueues = IntStream.rangeClosed(1, parallelism)
                .boxed()
                .collect(Collectors.toMap(
                        Function.identity(),
                        i -> new LinkedBlockingQueue<>(getQueueSize(true))
                ));
    }

    @Override
    protected void initializeConverters() {
        log.info("Converters are being  created, because Source [{}] and Sink [{}] data types are different non basic types", source.getType(), sink.getType());
        converterWorkers = new HashMap<>();

        if (converterSupplier == null) {
            throw new DittoRuntimeException("Converters were not supplied to orchestration. This is needed for job, which has to convert data");
        }

        WorkerThreadFactory<ConverterWorker<SRC, SNK>> converterThreadFactory =
                new WorkerThreadFactory<>(jobId,
                        LoggingUncaughtExceptionHandler.withChild(
                                new StoppingUncaughtExceptionHandler(this)),
                        WorkerThread.WorkerType.CONVERTER);

        for (Map.Entry<Integer, BlockingQueue<StreamElement<SRC>>> entry : sourceQueues.entrySet()) {
            converterWorkers.put(
                    entry.getKey(),
                    converterThreadFactory.modifyThread(
                            new ConverterWorker<>(
                                    workerEventCommunicator,
                                    entry   .getValue(),
                                    sinkQueues.get(entry.getKey()),
                                    converterSupplier.get()),
                            entry.getKey())
            );
        }

        // now initialize converters
        converterWorkers.values().forEach(ConverterWorker::initThread);
    }
}
