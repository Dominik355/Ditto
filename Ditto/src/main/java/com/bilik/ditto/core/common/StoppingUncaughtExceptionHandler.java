package com.bilik.ditto.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoppingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(StoppingUncaughtExceptionHandler.class);

    private final Stoppable stoppableObject;

    public StoppingUncaughtExceptionHandler(Stoppable stoppableObject) {
        this.stoppableObject = stoppableObject;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            log.error("Stopping " + stoppableObject.getClass().getName(), e);
            this.stoppableObject.stop();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
