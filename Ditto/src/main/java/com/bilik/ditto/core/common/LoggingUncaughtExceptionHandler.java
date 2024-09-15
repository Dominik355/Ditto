package com.bilik.ditto.core.common;

import com.bilik.ditto.core.exception.DittoRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Thread.UncaughtExceptionHandler;

/**
 * Just logs exception. If parent defined, supply exception to it, otherwise rethrow as runtimeException
 */
public class LoggingUncaughtExceptionHandler implements UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingUncaughtExceptionHandler.class);

    private UncaughtExceptionHandler parent;

    public LoggingUncaughtExceptionHandler() {
    }

    public LoggingUncaughtExceptionHandler(UncaughtExceptionHandler parent) {
        this.parent = parent;
    }

    public static LoggingUncaughtExceptionHandler withChild(UncaughtExceptionHandler parent) {
        return new LoggingUncaughtExceptionHandler(parent);
    }

    @Override
    public void uncaughtException(Thread thread, Throwable ex) {
        log.error("Error occured in thread {}. Exception: {}", thread.getName(), getStackTrace(ex));
        if (this.parent != null) {
            this.parent.uncaughtException(thread, ex);
        } else {
            throw new DittoRuntimeException(ex);
        }
    }

    public static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

}
