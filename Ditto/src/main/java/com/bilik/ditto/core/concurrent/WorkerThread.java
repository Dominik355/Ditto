package com.bilik.ditto.core.concurrent;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent.WorkerState;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;

import java.util.Objects;
import java.util.StringJoiner;

public abstract class WorkerThread extends Thread {

    /**
     * Number identifying this thread (instead of name - that is just informative).
     * Used by Sink/Source to identify its workers
     * This might be replaced with thread's ID (see Thread.getId()) but this way we can use our own
     * identifier, which would make more sense when, debugging. It is more
     * straightforward seeing id's like [1, 2, 3, 4] than [231314, 3112, 55, 65948].
     */
    private int workerNumber;

    // 'effectively final' - once declared, can not be changed
    private WorkerType workerType;

    protected final WorkerEventProducer workerEventProducer;

    protected volatile boolean running;

    protected volatile boolean hasBeenInitialized;

    protected WorkerThread(WorkerEventProducer workerEventProducer) {
        this.workerEventProducer = Objects.requireNonNull(workerEventProducer, "WorkerThread's EventProducer can not be null");
    }

    public void setType(WorkerType workerType) {
        if (this.workerType == null) {
            this.workerType = workerType;
        }
    }

    public WorkerType getType() {
        return workerType;
    }

    public void setWorkerNumber(int workerNumber) {
        this.workerNumber = workerNumber;
    }

    public int getWorkerNumber() {
        return this.workerNumber;
    }

    public void shutdown() {
        this.running = false;
    }

    public boolean isRunning() {
        return this.running;
    }

    /**
     * @return true if thread was initialized successfully, otherwise false
     */
    public boolean initThread() {
        this.hasBeenInitialized = true;
        return true;
    }

    protected void produceEvent(WorkerState state) {
        workerEventProducer.sendEvent(new WorkerEvent(state, this));
    }

    protected void produceErrorEvent(Exception e) {
        WorkerEvent event = new WorkerEvent(WorkerEvent.WorkerState.ERROR, this);
        event.setError(e.getMessage());
        workerEventProducer.sendEvent(event);
    }

    protected void copyProperties(WorkerThread destinationThread, String nameSuffix) {
        destinationThread.setDaemon(this.isDaemon());
        destinationThread.setUncaughtExceptionHandler(this.getUncaughtExceptionHandler());
        destinationThread.setWorkerNumber(this.workerNumber);
        destinationThread.setName(this.getName() + "-" + nameSuffix);
        destinationThread.setType(this.workerType);
    }

    public enum WorkerType {
        SOURCE("source"),
        CONVERTER("converter"),
        SINK("sink");

        public final String name;

        WorkerType(String name) {
            this.name = name;
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", WorkerThread.class.getSimpleName() + "[", "]")
                .add("name=" + getName())
                .add("pritority=" + getPriority())
                .add("group=" + (getThreadGroup() != null ? getName() : "NONE"))
                .add("workerNumber=" + workerNumber)
                .add("type=" + workerType)
                .toString();
    }
}
