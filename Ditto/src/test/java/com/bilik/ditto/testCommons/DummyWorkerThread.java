package com.bilik.ditto.testCommons;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;

public class DummyWorkerThread extends WorkerThread {

    private boolean initialized;

    public DummyWorkerThread(WorkerEventProducer eventProducer) {
        super(eventProducer);
    }

    public DummyWorkerThread() {
        super(WorkerEventProducer.dummy());
    }

    @Override
    public void run() {
        running = true;
        while (running) {
            produceEvent(WorkerEvent.WorkerState.STARTED);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean initThread() {
        return initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }
}