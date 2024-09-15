package com.bilik.ditto.gateway.server;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class PoolThreadFactory implements ThreadFactory {

    private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();

    private final AtomicLong threadNumber = new AtomicLong(1);

    private final String prefix;

    private final Thread.UncaughtExceptionHandler exceptionHandler;

    public PoolThreadFactory(String prefix) {
        this(prefix, null);
    }

    public PoolThreadFactory(String prefix, Thread.UncaughtExceptionHandler exceptionHandler) {
        this.prefix = prefix;
        this.exceptionHandler = exceptionHandler;
    }

    public Thread newThread(Runnable r) {
        Thread t = defaultThreadFactory.newThread(r);
        t.setName(prefix + threadNumber.getAndIncrement());
        t.setDaemon(true);

        if (exceptionHandler != null) {
            t.setUncaughtExceptionHandler(exceptionHandler);
        }

        return t;
    }
}
