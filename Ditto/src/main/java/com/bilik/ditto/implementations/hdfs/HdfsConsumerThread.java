package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.metric.Counter;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.metric.ThreadSafeCounter;
import com.bilik.ditto.core.transfer.Handover;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class HdfsConsumerThread extends WorkerThread {

    private static final Logger log = LoggerFactory.getLogger(HdfsConsumerThread.class);

    private final HdfsFileDownloader downloader;

    private final Handover<PathWrapper> handover;

    private final HdfsSplit split;

    private final CounterAggregator counterAggregator;

    private Counter readObjects;

    public HdfsConsumerThread(WorkerEventProducer workerEventProducer,
                              HdfsFileDownloader downloader,
                              Handover<PathWrapper> handover,
                              HdfsSplit split,
                              CounterAggregator counterAggregator) {
        super(workerEventProducer);
        this.downloader = downloader;
        this.handover = handover;
        this.split = split;
        this.counterAggregator = counterAggregator;
    }

    @Override
    public boolean initThread() {
        super.initThread();
        this.readObjects = counterAggregator.addCounter(getName() + "_read_objects", new ThreadSafeCounter());
        log.info("HdfsConsumerThread has been initialized");
        return true;
    }

    @Override
    public void run() {
        if (running) {
            log.warn("HdfsConsumerThread [{}] is already in running state", getName());
            return;
        } else if (!hasBeenInitialized) {
            throw new DittoRuntimeException("WorkerThread has not been initialized yet!");
        } else {
            running = true;
            produceEvent(WorkerEvent.WorkerState.STARTED);
            log.info("HdfsConsumerThread is running");
        }

        try {
            PathWrapper currentPath = null;
            Iterator<Path> iterator = split.iterator();

            while (running && iterator.hasNext()) {
                if (currentPath == null) {
                    currentPath = new PathWrapper(downloader.download(iterator.next()));
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
                log.info("HdfsConsumerThread has read all of its data.");
            }

        } catch (Exception ex) {
            log.error("Error occured in HdfsConsumerThread: {}. Sending error over handover to let main thread know about it", ex.getMessage());
            handover.reportError(ex);
        } finally {
            // make sure the handover is closed
            handover.closeSoftly();
        }

        log.info("HdfsConsumerThread is finished. It has downloaded {}/{} files", readObjects.getCount(), split.size());
    }

    @Override
    public void shutdown() {
        super.shutdown();
        // this wakes up the consumer if it is blocked handing over records
        handover.wakeupProducer();
    }
}
