package com.bilik.ditto.core.concurrent.threadCommunication;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueWorkerEventCommunicator implements WorkerEventCommunicator {

    /**
     * just to make sure, we won't overflow this queue with some bug.
     * Each worker should not send more than 2 events during his lifetime.
     * So the capacity of 100_000 should be sufficient.
     * Also, it does not take up memory, since itn uses a linkeQueue and not an arrayQueue.
     * LinkedBlockingQueue has different locks for writing and reading (no crossover)
     */
    private static final int DEFAULT_SIZE = 100_000;
    private static final long DEFAULT_TIMEOUT_SECONDS = Long.MAX_VALUE; // we want to wait for next event no matter what

    private Long timeout;
    private TimeUnit timeUnit;

    private final BlockingQueue<WorkerEvent> sinkQueue = new LinkedBlockingQueue<>(DEFAULT_SIZE);

    public QueueWorkerEventCommunicator() {}

    /**
     * For testing
     */
    public QueueWorkerEventCommunicator(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.timeUnit = unit;
    }

    @Override
    public void sendEvent(WorkerEvent event) {
        sinkQueue.add(event);
    }

    @Override
    public WorkerEvent receiveEvent() throws InterruptedException {
        return sinkQueue.poll(
                timeout == null ? DEFAULT_TIMEOUT_SECONDS : timeout,
                timeUnit == null ? TimeUnit.SECONDS : timeUnit);
    }

}
