package com.bilik.ditto.core.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class SinkProducersThreadFactory implements ThreadFactory {

    private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String jobId;

    public SinkProducersThreadFactory(String jobId, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        this.jobId = jobId;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = defaultThreadFactory.newThread(r);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        thread.setDaemon(true);
        thread.setName(jobId + "-Sink_Producer-" + threadNumber.getAndIncrement());
        return thread;
    }
}
