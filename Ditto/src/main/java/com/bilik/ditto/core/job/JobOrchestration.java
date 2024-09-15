package com.bilik.ditto.core.job;

import com.bilik.ditto.core.common.Stoppable;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.configuration.DittoConfiguration;
import com.bilik.ditto.core.convertion.ConverterWorker;
import com.bilik.ditto.core.concurrent.threadCommunication.OrchestrationEventLoop;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventCommunicator;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import com.bilik.ditto.core.metric.CounterAggregator;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @param <SRC> - source type
 * @param <SNK> - sink type
 * Those 2 might be same
 */
public abstract class JobOrchestration<SRC, SNK> implements Runnable, Stoppable {

    private static final Logger log = LoggerFactory.getLogger(JobOrchestration.class);

    protected static final int DEFAULT_QUEUE_SIZE = 100_000;

    private final Runnable sourceOnFinish = this::sourceFinished;
    private final Runnable sinkOnFinish = this::sinkFinished;

    protected OrchestrationEventLoop orchestrationEventLoop;
    protected Source<SRC> source;
    protected Sink<SNK> sink;

    /*
    If there is no data conversion (Source and Sink are using same DataType),
    then both of these maps holds the same queue instances
     */
    protected Map<Integer, BlockingQueue<StreamElement<SRC>>> sourceQueues;
    protected Map<Integer, BlockingQueue<StreamElement<SNK>>> sinkQueues;

    protected Map<Integer, ConverterWorker<SRC, SNK>> converterWorkers;

    protected final DittoConfiguration.OrchestrationConfig configuration;
    protected final WorkerEventCommunicator workerEventCommunicator;

    private final List<WorkerEvent> workerEvents = new ArrayList<>(); // keep history of workerEvents

    protected final String jobId;

    protected int parallelism;

    private volatile boolean isFinished;

    private final Consumer<JobOrchestration<?,?>> whenFinished;

    private final Instant creationTime;
    private Instant startTime;
    private Instant runningTime;
    private Instant finishTime;
    private String error;

    JobOrchestration(Source<SRC> source,
                     Sink<SNK> sink,
                     DittoConfiguration.OrchestrationConfig configuration,
                     WorkerEventCommunicator workerEventCommunicator,
                     String jobId,
                     int parallelism,
                     Consumer<JobOrchestration<?,?>> whenFinished) {
        this.source = source;
        this.sink = sink;
        this.configuration = configuration;
        this.workerEventCommunicator = workerEventCommunicator;
        this.jobId = jobId;
        this.parallelism = parallelism;
        this.whenFinished = whenFinished;
        this.creationTime = Instant.now();
    }


    protected abstract void initializeQueues();
    protected abstract void initializeConverters();

    /**
     * @throws RuntimeException, if orchestration could not be initialized
     */
    public void initializeOrchestration() {
        // initialize queues. Because we can cast 2 level generic into each other. It is doen this way
        // queues are initialized based on starting parallelism. Then after initializing source, we remove
        // not needed elements. For different SRC and SNK covnerters are initialized and both queues are different.
        // For monotonous job, both maps holds same instances of queues.

        initializeQueues();

        // initialize source and return numbers of initialized workers
        Collection<Integer> initializedSourceWorkers = source.initialize(sourceQueues);
        this.parallelism = initializedSourceWorkers.size();

        // remove not initialized worker numbers from workerMap
        // here we know how many sink workers we need to create and what parallelism sink is going to have.
        sourceQueues.entrySet().removeIf(entry -> !initializedSourceWorkers.contains(entry.getKey()));
        sinkQueues.entrySet().removeIf(entry -> !initializedSourceWorkers.contains(entry.getKey()));

        if (sourceQueues.isEmpty()) {
            throw DittoRuntimeException.of("There are no initialized source workers for job {}", jobId);
        }

        // if job is converting data, we have to create converters
        initializeConverters();

        log.info("Based on initialized source workers, these sink workers are going to be initialized: {}", sinkQueues.keySet());

        sink.initialize(sinkQueues);

        initializeEventLoopThread();

        this.source.setOnFinish(sourceOnFinish);
        this.sink.setOnFinish(sinkOnFinish);

        log.info("Initialization of JobOrchestration {} has finished. Everything ready to start. Real parallelism is {}/{}", jobId, sinkQueues.size(), parallelism);
    }

    /**
     * STARTED - not handled
     * FINISHED - passed to Source/Sink
     * ERROR - close job
     * @param event
     */
    public void handleEvent(WorkerEvent event) {
        log.info("Handling event {}", event);
        workerEvents.add(event);

        if (WorkerEvent.WorkerState.FINISHED.equals(event.getWorkerState())) {
            switch (event.getType()) {
                case SINK -> sink.workerFinished(event.getThreadNum());
                case SOURCE -> source.workerFinished(event.getThreadNum());
            }
        } else if (WorkerEvent.WorkerState.ERROR.equals(event.getWorkerState())) {
            log.info("Closing orchestration because error event occurred: {}", event);
            this.error = event.getError();
            try {
                stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void sourceFinished() {
        log.info("Source has finished its work. Lets send closing element to all the queues");
        for (var entry : sourceQueues.entrySet()) {
            log.info("Sending closing element to queue num {}", entry.getKey());
            try {
                boolean send = entry.getValue().offer(StreamElement.last(), 60, TimeUnit.SECONDS);
                if (!send) {
                    throw new DittoRuntimeException("We were not able to send closing element to source queue " + entry.getKey() + "in 20 seconds!");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void sinkFinished() {
        log.info("Sink has finished its work. Job is done and everything can be closed.");
        try {
            stop();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DittoRuntimeException(e);
        }
    }

    private void initializeEventLoopThread() {
        orchestrationEventLoop = new OrchestrationEventLoop(workerEventCommunicator, this);
        orchestrationEventLoop.setDaemon(true);
        orchestrationEventLoop.setName("Event-Loop-" + jobId);
    }

    @Override
    public void run() {
        startTime = Instant.now();
        orchestrationEventLoop.start(); // start first so we can instantly catch any events
        if (MapUtils.isNotEmpty(converterWorkers)) {
            for (WorkerThread thread : converterWorkers.values()) {
                log.info("Starting Converter thread {}", thread);
                thread.start();
            }
        }
        source.start(); // source first
        sink.start(); // sink and we are ready to go
        log.info("JobOrchestration with id {} is running !", jobId);
        runningTime = Instant.now();
    }

    @Override
    public void stop() throws Exception {
        log.info("Closing JobOrchestration {}", jobId);
        source.stop();
        if (MapUtils.isNotEmpty(converterWorkers)) {
            for (ConverterWorker thread : converterWorkers.values()) {
                log.info("Stopping Converter thread {}", thread);
                thread.shutdown();
            }
        }
        sink.stop();
        isFinished = true;
        finishTime = Instant.now();
        if (whenFinished != null) {
            whenFinished.accept(this);
        }

        // this method is called from eventLoop thread, so terminate after whenFinished() has been called
        orchestrationEventLoop.terminate();
    }

    public String getJobId() {
        return jobId;
    }

    protected int getQueueSize(boolean isConverting) {
        int queueSize = DEFAULT_QUEUE_SIZE;
        if (configuration != null && configuration.getQueueSize() != null && configuration.getQueueSize() > 0) {
            queueSize = configuration.getQueueSize();
        }

        int finalSize;
        if (parallelism > queueSize) {
            finalSize = queueSize;
        } else {
            finalSize = queueSize / parallelism;

            // converting orchestration has every queue separated into 2 - before and after the converter
            if (isConverting) {
                finalSize = finalSize / 2;
            }
        }

        log.info("Total QueueSize is {}, job has parallelism {} and its converting job {}, so Queue between source and sink will have size {}", queueSize, parallelism, isConverting, finalSize);
        return finalSize;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public CounterAggregator getSinkCounterAggregator() {
        return sink.getCounterAggregator();
    }

    public CounterAggregator getSourceCounterAggregator() {
        return source.getCounterAggregator();
    }

    public int getParallelism() {
        return parallelism;
    }

    public List<WorkerEvent> getWorkerEventsView() {
        return Collections.unmodifiableList(workerEvents);
    }

    public Map<Integer, Integer> getSourceQueueSizes() {
        return sourceQueues.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> entry.getValue().size()
                ));
    }

    public Map<Integer, Integer> getSinkQueueSizes() {
        return sinkQueues.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> entry.getValue().size()
                ));
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getRunningTime() {
        return runningTime;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    public String getError() {
        return error;
    }

    public DittoConfiguration.OrchestrationConfig getConfiguration() {
        return configuration;
    }
}
