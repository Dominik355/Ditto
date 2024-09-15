package com.bilik.ditto.core.concurrent.threadCommunication;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.WorkerThread.WorkerType;
import com.bilik.ditto.core.exception.DittoRuntimeException;

import java.io.Serial;
import java.io.Serializable;
import java.util.StringJoiner;

/**
 * posielanie statov smerom od workrov do managera. Manager teda musi byt beziaci thread, ktory bude prijimat spravy
 * a na zaklade nich mat definovane, co ma robit
 */
public class WorkerEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 0L;

    private final WorkerType workerType;
    private final WorkerState workerState;
    private final int threadNum;
    private String error;

    public WorkerEvent(WorkerState workerState, WorkerThread workerThread) {
        this.threadNum = workerThread.getWorkerNumber();
        this.workerType = workerThread.getType();
        this.workerState = workerState;
    }

    /**
     * Sink's producer ThreadPool creates Error event for every uncaught exception.
     * Naming is not best, because  this was intended to be used by WorkerEvent,
     * but I ended up using ThreadPool at sink's side. But it's a way to terminate
     * job if exception occurs in ThreadPool
     */
    public WorkerEvent(WorkerState workerState, WorkerType workerType) {
        this.workerState = workerState;
        this. workerType = workerType;
        this.threadNum = -1; // I will know that its sink's ThreadPool
    }

    public void setError(String error) {
        if (!WorkerState.ERROR.equals(workerState)) {
            throw new DittoRuntimeException("You can not add error to WorkerEvent, which does not have ERROR state");
        }
        this.error = error;
    }

    public String getError() {
        return error;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public WorkerType getType() {
        return workerType;
    }

    public WorkerState getWorkerState() {
        return workerState;
    }

    public boolean isSource() {
        return WorkerType.SOURCE.equals(workerType);
    }

    public boolean isSink() {
        return WorkerType.SINK.equals(workerType);
    }



    @Override
    public String toString() {
        return new StringJoiner(", ", WorkerEvent.class.getSimpleName() + "[", "]")
                .add("threadNum=" + threadNum)
                .add("type=" + workerType)
                .add("workerState=" + workerState)
                .toString();
    }

    /**
     * This has nothing to do with enum Thread.State.
     * This is internal represantation of events, which thread can send to it's maanger.
     * These events should trigger events like -termination of other threads, or finishing of other threads
     */
    public enum WorkerState {
        STARTED, // thread has started and is doing its work
        FINISHED, // thread finished normally
        ERROR; // error occurred, which causes thread to stop
    }

}
