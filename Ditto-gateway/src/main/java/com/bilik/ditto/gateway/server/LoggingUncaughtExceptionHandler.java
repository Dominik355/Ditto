package com.bilik.ditto.gateway.server;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just logs exception. If parent defined, supply exception to it, otherwise rethrow as runtimeException
 */
public class LoggingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingUncaughtExceptionHandler.class);

    private Thread.UncaughtExceptionHandler parent;

    public LoggingUncaughtExceptionHandler() {
    }

    public LoggingUncaughtExceptionHandler(Thread.UncaughtExceptionHandler parent) {
        this.parent = parent;
    }

    public static LoggingUncaughtExceptionHandler withChild(Thread.UncaughtExceptionHandler parent) {
        return new LoggingUncaughtExceptionHandler(parent);
    }

    @Override
    public void uncaughtException(Thread thread, Throwable ex) {
        log.error("Error occured in thread {}. Exception: {}", thread.getName(), ExceptionUtils.getStackTrace(ex));
        if (this.parent != null) {
            this.parent.uncaughtException(thread, ex);
        } else {
            throw new RuntimeException(ex);
        }
    }

}