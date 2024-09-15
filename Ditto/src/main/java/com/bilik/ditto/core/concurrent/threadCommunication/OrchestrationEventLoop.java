package com.bilik.ditto.core.concurrent.threadCommunication;

import com.bilik.ditto.core.job.JobOrchestration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class OrchestrationEventLoop extends Thread {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationEventLoop.class);

    private volatile boolean running;

    private final WorkerEventReceiver eventReceiver;

    private final JobOrchestration<?, ?> jobOrchestration;

    public OrchestrationEventLoop(WorkerEventReceiver eventReceiver, JobOrchestration<?, ?> jobOrchestration) {
        this.eventReceiver = eventReceiver;
        this.jobOrchestration = jobOrchestration;
    }

    @Override
    public void run() {
        log.info("Starting OrchestrationEventLoop");
        try {
            running = true;

            while(running) {
                WorkerEvent event = eventReceiver.receiveEvent();
                if (event != null) {
                    jobOrchestration.handleEvent(event);
                }
            }
        } catch (InterruptedException ex) {
            // nothing, we have to interrupt polling in order to finish thread
        } catch (Exception ex) {
            log.warn("Exception occured in OrchestrationEventLoop. Exception: {}", Arrays.toString(ex.getStackTrace()));
        }
        log.info("OrchestrationEventLoop has finished");
    }

    public void terminate() {
        running = false;
        interrupt();
    }

}
