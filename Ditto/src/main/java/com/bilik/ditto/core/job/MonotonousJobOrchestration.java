package com.bilik.ditto.core.job;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventCommunicator;
import com.bilik.ditto.core.configuration.DittoConfiguration;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MonotonousJobOrchestration<T> extends JobOrchestration<T, T> {

    private static final Logger log = LoggerFactory.getLogger(ConvertingJobOrchestration.class);

    public MonotonousJobOrchestration(
            Source<T> source,
            Sink<T> sink,
            DittoConfiguration.OrchestrationConfig configuration,
            WorkerEventCommunicator workerEventCommunicator,
            String jobId,
            int parallelism,
            Consumer<JobOrchestration<?,?>> whenFinished) {
        super(source, sink, configuration, workerEventCommunicator, jobId, parallelism, whenFinished);
    }

    @Override
    protected void initializeQueues() {
        this.sourceQueues = IntStream.rangeClosed(1, parallelism)
                .boxed()
                .collect(Collectors.toMap(
                        Function.identity(),
                        i -> new LinkedBlockingQueue<>(getQueueSize(false))
                ));

        this.sinkQueues = new HashMap<>();
        sinkQueues.putAll(sourceQueues);
    }

    @Override
    protected void initializeConverters() {

    }

}
