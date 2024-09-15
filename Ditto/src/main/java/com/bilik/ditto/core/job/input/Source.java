package com.bilik.ditto.core.job.input;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.job.SourceSinkParent;
import com.bilik.ditto.core.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public abstract class Source<T> extends SourceSinkParent<T> {

    private static final Logger log = LoggerFactory.getLogger(Source.class);

    protected int parallelism;

    protected final Type<T> type;

    protected final Set<Integer> finishedWorkers = new CopyOnWriteArraySet<>();

    protected Map<Integer, WorkerThread> workerThreads;

    protected final String jobId;

    protected final CounterAggregator counterAggregator = new CounterAggregator();

    protected final WorkerEventProducer eventProducer;

    protected Source(int parallelism, Type<T> type, String jobId, WorkerEventProducer eventProducer) {
        this.parallelism = parallelism;
        this.type = type;
        this.jobId = jobId;
        this.eventProducer = eventProducer;
    }

    @Override
    public boolean start() {
        if (workerThreads == null || workerThreads.isEmpty()) {
            log.error("Worker Threads are empty for KafkaSource. This should not happen");
            return false;
        }

        for (WorkerThread thread : workerThreads.values()) {
            log.info("Starting Thread {}", thread);
            thread.start();
        }

        return true;
    }

    @Override
    public void stop() throws Exception {
        for (WorkerThread thread : workerThreads.values()) {
            log.info("Stopping KafkaWorker thread {}", thread.getName());
            thread.shutdown();
        }
    }

    @Override
    public CounterAggregator getCounterAggregator() {
        return counterAggregator;
    }

    @Override
    public Type<T> getType() {
        return type;
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

    public abstract static class SourceBuilder<T, B extends SourceBuilder<T, B>> {
        protected int parallelism;
        protected Type<T> type;
        protected String jobId;
        protected WorkerEventProducer eventProducer;

        public B parallelism(int parallelism) {
            this.parallelism = parallelism;
            return self();
        }

        public B type(Type<T> type) {
            this.type = type;
            return self();
        }

        public B jobId(String jobId) {
            this.jobId = jobId;
            return self();
        }

        public B eventProducer(WorkerEventProducer eventProducer) {
            this.eventProducer = eventProducer;
            return self();
        }

        public abstract Source<T> build();

        protected abstract B self();
    }

}
