package com.bilik.ditto.core.job.output;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.output.accumulation.SinkAccumulator;
import com.bilik.ditto.core.job.StreamElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * type of SinkWorker is defined by its accumulator
 */
public class SinkWorker<T> extends WorkerThread {

    private static final Logger log = LoggerFactory.getLogger(SinkWorker.class);

    private final BlockingQueue<StreamElement<T>> sourceQueue;

    private final SinkAccumulator<T> sinkAccumulator;

    private final ExecutorService executor;

    public SinkWorker(WorkerEventProducer workerEventProducer,
                      BlockingQueue<StreamElement<T>> sourceQueue,
                      SinkAccumulator<T> sinkAccumulator,
                      ExecutorService executor) {
        super(workerEventProducer);
        this.sourceQueue = Objects.requireNonNull(sourceQueue);
        this.sinkAccumulator = Objects.requireNonNull(sinkAccumulator);
        this.executor = Objects.requireNonNull(executor);
        log.info("SinkWorker obtained executor: " + executor);
    }

    @Override
    public boolean initThread() {
        return super.initThread();
    }

    @Override
    public void run() {
        if (running) {
            log.warn("SinkWorker [{}] is already in running state", getName());
            return;
        } else if (!hasBeenInitialized) {
            throw new DittoRuntimeException("WorkerThread has not been initialized yet!");
        } else {
            running = true;
            produceEvent(WorkerEvent.WorkerState.STARTED);
            log.info("SinkWorker[{}] has started", getName());
        }

        try {
            while (running) {

                StreamElement<T> element = sourceQueue.poll(Long.MAX_VALUE, TimeUnit.SECONDS);
                if (element.isLast()) {
                    log.info("SinkWorker has reached last element. Finishing this iteration and setting running flag to false");
                    if (sinkAccumulator.size() > 0) { // check if there are any accumulated elements, that needs to be sunk
                        executor.execute(sinkAccumulator.onFinish());
                    }
                    break;
                }

                sinkAccumulator.collect(element);
                if (sinkAccumulator.isFull()) {
                    executor.execute(sinkAccumulator.onFull());
                }

            }
        } catch (InterruptedException ex) {
            log.info("SinkWorker has been interrupted");
        } catch (Exception e) {
            produceErrorEvent(e);
            throw new DittoRuntimeException(e);
        }

        produceEvent(WorkerEvent.WorkerState.FINISHED);
        log.info("SinkWorker[{}] has finished", getName());
    }

    @Override
    public void shutdown() {
        super.shutdown();
        interrupt();
        try {
            sinkAccumulator.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
