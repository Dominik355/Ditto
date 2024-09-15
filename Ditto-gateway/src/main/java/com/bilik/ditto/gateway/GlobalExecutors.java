package com.bilik.ditto.gateway;

import com.bilik.ditto.gateway.server.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.gateway.server.PoolThreadFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GlobalExecutors {

    private final ThreadPoolExecutor httpClientPool, restServerPool;

    private GlobalExecutors() {}

    {
        this.httpClientPool = new ThreadPoolExecutor(1,
                6,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new PoolThreadFactory("rest-client-", new LoggingUncaughtExceptionHandler()));
        this.restServerPool = new ThreadPoolExecutor(
                1,
                15, // this maxpoolsize should be equal to num of running replicas
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new PoolThreadFactory("rest-server-thread-", new LoggingUncaughtExceptionHandler()));
    }

    public ThreadPoolExecutor httpClientPool() {
        return httpClientPool;
    }

    public ThreadPoolExecutor restServerPool() {
        return restServerPool;
    }

    private static class LazySingleton {
        static final GlobalExecutors instance = new GlobalExecutors();
    }

    public static GlobalExecutors getInstance() {
        return LazySingleton.instance;
    }

}
