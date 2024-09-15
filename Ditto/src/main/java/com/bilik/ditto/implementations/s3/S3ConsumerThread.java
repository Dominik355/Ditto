package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.metric.Counter;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.metric.ThreadSafeCounter;
import com.bilik.ditto.core.transfer.Handover;
import com.bilik.ditto.implementations.s3.S3Downloader.FileS3Downloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class S3ConsumerThread extends WorkerThread {

    private static final Logger log = LoggerFactory.getLogger(S3ConsumerThread.class);

    private final FileS3Downloader s3Downloader;

    private final Handover<PathWrapper> handover;

    private final S3Split split;

    private final CounterAggregator counterAggregator;

    private Counter readObjects;

    public S3ConsumerThread(WorkerEventProducer workerEventProducer,
                            FileS3Downloader s3Downloader,
                            Handover<PathWrapper> handover,
                            S3Split split,
                            CounterAggregator counterAggregator) {
        super(workerEventProducer);
        this.s3Downloader = s3Downloader;
        this.handover = handover;
        this.split = split;
        this.counterAggregator = counterAggregator;
    }

    /**
     * There is no need to initialize anything really.
     * Consumer has assigned list of objects to work with, which for sure exists (not unlike for KafkaConsumerThread)
     */
    @Override
    public boolean initThread() {
        super.initThread();
        this.readObjects = counterAggregator.addCounter(getName() + "_read_objects", new ThreadSafeCounter());
        log.info("S3ConsumerThread has been initialized");
        return true;
    }

    @Override
    public void run() {
        if (running) {
            log.warn("S3ConsumerThread [{}] is already in running state", getName());
            return;
        } else if (!hasBeenInitialized) {
            throw new DittoRuntimeException("WorkerThread has not been initialized yet!");
        } else {
            running = true;
            produceEvent(WorkerEvent.WorkerState.STARTED);
            log.info("S3ConsumerThread is running");
        }

        try {
            PathWrapper currentPath = null;
            Iterator<String> iterator = split.iterator();

            while (running && iterator.hasNext()) {

                if (currentPath == null) {
                    currentPath = new PathWrapper(s3Downloader.download(iterator.next()));
                    readObjects.inc();
                }

                try {
                    handover.produce(currentPath);
                    currentPath = null;
                } catch (Handover.WakeupException e) {
                    // fall through the loop
                }
            }

            if (!iterator.hasNext()) {
                log.info("S3ConsumerThread has read all of its data.");
            }

        } catch (Exception ex) {
            log.error("Error occured in S3ConsumerThread: {}. Sending error over handover to let main thread know about it", ex.getMessage());
            handover.reportError(ex);
        } finally {
            // make sure the handover is closed
            handover.closeSoftly();
        }

        log.info("S3ConsumerThread is finished. It has consumed {}/{} objects", readObjects.getCount(), split.size());
    }

    @Override
    public void shutdown() {
        super.shutdown();
        // this wakes up the consumer if it is blocked handing over records
        handover.wakeupProducer();
    }
}
