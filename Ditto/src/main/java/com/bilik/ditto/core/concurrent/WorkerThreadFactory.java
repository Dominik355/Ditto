package com.bilik.ditto.core.concurrent;

import com.bilik.ditto.core.util.StringUtils;
import com.bilik.ditto.core.concurrent.WorkerThread.WorkerType;
import com.bilik.ditto.core.util.Preconditions;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Objects;

public class WorkerThreadFactory<T extends WorkerThread> {

    private final String jobId;
    private final WorkerType workerType; // either 'source' or 'sink'
    protected final ThreadGroup threadGroup;
    private final UncaughtExceptionHandler uncaughtExceptionHandler;


    public WorkerThreadFactory(String jobId, WorkerType workerType, ThreadGroup threadGroup, UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.jobId = Preconditions.requireNotBlank(jobId, "JobId can not be null");
        this.workerType = Objects.requireNonNull(workerType, "Worker type can not be null");
        this.threadGroup = Objects.requireNonNull(threadGroup, "ThreadGroup can not be null");
        this.uncaughtExceptionHandler = Objects.requireNonNull(uncaughtExceptionHandler, "UncaughtExceptionHandler can not be null");
    }

    public WorkerThreadFactory(String jobId, UncaughtExceptionHandler uncaughtExceptionHandler, WorkerType workerType) {
        this(jobId, workerType, Thread.currentThread().getThreadGroup(), uncaughtExceptionHandler);
    }

    /**
     * Why does it return thread, when it's modified anyway ?
     * Because I use it when creating a thread, when I want to assign it for the first time
     */
    public T modifyThread(T thread, int workerNum) {
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        thread.setWorkerNumber(workerNum);
        thread.setName(StringUtils.joinNullable("-", jobId, workerType.name, String.valueOf(workerNum)));
        thread.setType(workerType);
        return thread;
    }

}
