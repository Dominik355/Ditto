package com.bilik.ditto.core.job.output;

import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.core.common.StoppingUncaughtExceptionHandler;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.WorkerThreadFactory;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.SourceSinkParent;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.job.output.accumulation.AccumulatorProvider;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.job.StreamElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sink was created in a way, that it can have common class
 */
public class Sink<T> extends SourceSinkParent<T> {

    private static final Logger log = LoggerFactory.getLogger(Sink.class);

    /**
     * This is not final, because it is determined on the basis of initialized source workers.
     * Value assigned in initialize() method
     */
    private int parallelism;

    private final Type<T> type;

    private final String jobId;

    private final CounterAggregator counterAggregator = new CounterAggregator();

    private Map<Integer, SinkWorker<T>> workerThreads;

    /*
    This ain't final, because it needs to have uncaughException handler, which
    closes this particular sink. So Sink has ot be created first and then
    supplied to handler and to executor
     */
    private ExecutorService executor;

    private final AccumulatorProvider<T> accumulatorProvider;

    private final WorkerEventProducer eventProducer;

    private final Set<Integer> finishedWorkers = new CopyOnWriteArraySet<>();

    public Sink(Type<T> type,
                  String jobId,
                  WorkerEventProducer eventProducer,
                  ExecutorService executor,
                  AccumulatorProvider<T> accumulatorProvider) {
        this.type = type;
        this.jobId = jobId;
        this.eventProducer = eventProducer;
        this.executor = executor;
        this.accumulatorProvider = accumulatorProvider;
    }

//    public void setExecutor(ExecutorService executor) {
//        if (executor != null) {
//            throw new IllegalStateException("ExecutorService has already been defined for Sink of job " + jobId);
//        }
//        this.executor = executor;
//    }


    /**
     * calls executor shutdown first and then wait for it.
     * That's because sinkWorkers has Accumulators, which contain uploaders/producers which are used in runnables,
     * ran by executor. So closing that worker first might cause exception when running Runnable which sends data to final Sink.
     */
    @Override
    public void stop() throws Exception {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        for (SinkWorker<T> thread : workerThreads.values()) {
            log.info("Stopping SinkWorker thread {}", thread.getName());
            thread.shutdown();
        }
    }

    @Override
    public boolean start() {
        if (workerThreads == null || workerThreads.isEmpty()) {
            log.error("Worker Threads are empty for Sink. This should not happen");
            return false;
        }

        for (WorkerThread thread : workerThreads.values()) {
            log.info("Starting Thread {}", thread);
            thread.start();
        }

        return true;
    }

    /**
     * To check existence of topic, you need an admin client (partitionsFor() might create
     * topic if auto.create.topics.enable is enabled). Because I don't want to supply another
     * parameter just to do 1 check, topic's existence check is done at orchestration creation
     */
    @Override
    public Collection<Integer> initialize(Map<Integer, BlockingQueue<StreamElement<T>>> workerMap) {
        this.parallelism = workerMap.size();

        if (parallelism == 0) {
            throw new DittoRuntimeException("Kafka Sink with parallelism 0 can not be initialized");
        }

        WorkerThreadFactory<SinkWorker<T>> threadFactory =
                new WorkerThreadFactory<>(jobId,
                        LoggingUncaughtExceptionHandler.withChild(
                                new StoppingUncaughtExceptionHandler(this)),
                        WorkerThread.WorkerType.SINK);

        workerThreads = new HashMap<>();

        for (Map.Entry<Integer, BlockingQueue<StreamElement<T>>> entry : workerMap.entrySet()) {
            SinkWorker<T> workerThread = threadFactory.modifyThread(new SinkWorker<>(
                    eventProducer,
                    entry.getValue(),
                    accumulatorProvider.createAccumulator(entry.getKey()),
                    executor
            ), entry.getKey());

            boolean initialized = workerThread.initThread();
            if(!initialized) { //TODO - sinkworker ma prazdnu init fazu, pokial ju nebude potrebovat, tak vyhodit
                throw new DittoRuntimeException("Sinkworker was not initialized. workerThread: " + workerThread);
            }

            workerThreads.put(workerThread.getWorkerNumber(), workerThread);
        }

        return workerThreads.keySet();
    }

    @Override
    public CounterAggregator getCounterAggregator() {
        return counterAggregator;
    }

    @Override
    public synchronized void workerFinished(int workerId) {
        if (!workerThreads.containsKey(workerId)) {
            throw DittoRuntimeException.of("There is no SinkWorker with id {}", workerId);
        } else if (finishedWorkers.contains(workerId)) {
            log.info("Worker {} has already finished", workerId);
            return;
        }
        finishedWorkers.add(workerId);
        if (finishedWorkers.containsAll(workerThreads.keySet())) {
            onFinish.run();
        }
    }

    @Override
    public synchronized Set<Integer> finishedWorkers() {
        return Collections.unmodifiableSet(finishedWorkers);
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public Type<T> type() {
        return type;
    }
}
