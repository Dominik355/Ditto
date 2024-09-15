package com.bilik.ditto.core.common;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;

/**
 * Use in combination with LoggingUncaughtExceptionHandler
 */
public class ErrorEventProducingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private final WorkerEventProducer eventProducer;

    public ErrorEventProducingUncaughtExceptionHandler(WorkerEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        eventProducer.sendEvent(new WorkerEvent(WorkerEvent.WorkerState.ERROR, WorkerThread.WorkerType.SINK));
    }

}
