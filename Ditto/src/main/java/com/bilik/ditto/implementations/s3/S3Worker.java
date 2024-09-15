package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.FileReader;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.metric.Counter;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.metric.ThreadSafeCounter;
import com.bilik.ditto.core.job.Collector;
import com.bilik.ditto.core.transfer.Handover;
import com.bilik.ditto.core.util.FileUtils;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.implementations.s3.S3Downloader.FileS3Downloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class S3Worker<T> extends WorkerThread {

    private static final Logger log = LoggerFactory.getLogger(S3Worker.class);

    private final FileRWFactory<T> fileRWFactory;

    private final BlockingQueue<StreamElement<T>> sinkQueue;

    private final CounterAggregator counterAggregator;

    private final Handover<PathWrapper> handover;

    private final S3Split s3Split;

    private final FileS3Downloader s3Downloader;

    private S3ConsumerThread consumerThread;

    private Counter readRecords;
    private Counter readObjects;

    protected S3Worker(WorkerEventProducer workerEventProducer,
                       FileRWFactory<T> fileRWFactory,
                       BlockingQueue<StreamElement<T>> sinkQueue,
                       CounterAggregator counterAggregator,
                       S3Split s3Split,
                       FileS3Downloader s3Downloader) {
        super(workerEventProducer);
        this.fileRWFactory = fileRWFactory;
        this.sinkQueue = sinkQueue;
        this.counterAggregator = counterAggregator;
        this.s3Split = s3Split;
        this.s3Downloader = s3Downloader;

        this.handover = new Handover<>();
    }

    @Override
    public boolean initThread() {
        super.initThread();
        this.readObjects = counterAggregator.addCounter(getName() + "_read_objects", new ThreadSafeCounter());
        this.readRecords = counterAggregator.addCounter(getName() + "_read_records", new ThreadSafeCounter());
        this.consumerThread = new S3ConsumerThread(
                workerEventProducer,
                s3Downloader,
                handover,
                s3Split,
                counterAggregator
        );
        log.info("Initialized S3WorkerThread");

        // because we didn't use factory to create consumer thread, we have to set it this way
        copyProperties(this.consumerThread, "consumer");
        return this.consumerThread.initThread();
    }

    @Override
    public void run() {
        if (running) {
            log.warn("S3WorkerThread [{}] is already in running state", getName());
            return;
        } else if (!hasBeenInitialized) {
            throw new DittoRuntimeException("WorkerThread has not been initialized yet!");
        } else {
            running = true;
            produceEvent(WorkerEvent.WorkerState.STARTED);
            log.info("S3WorkerThread is running");
        }

        try {
            // start actual consumer
            consumerThread.start();

            Collector<T> collector = new Collector.QueueCollector<>(sinkQueue);

            while (running) {
                // blocks until we get the next records
                // it automatically re-throws exceptions encountered in the consumer thread
                PathWrapper pathWrapper = handover.pollNext();
                readObjects.inc();

                try (FileReader<T> reader = fileRWFactory.buildFileReader(pathWrapper)) {
                    T currentElement;
                    while ((currentElement = reader.next()) != null) {
                        readRecords.inc();
                        collector.collect(StreamElement.of(currentElement));
                    }
                }

                try {
                    FileUtils.deleteFile(pathWrapper.getNioPath());
                } catch (IOException ex) {
                    throw DittoRuntimeException.of(ex,"Failed to delete file {}", pathWrapper.getNioPath());
                }
            }
        } catch (Handover.ClosedException ex) {
            log.info("Handover has been closed. It means that the consumer has completed its work");
        } catch (Exception ex) {
            // TOOD - chceme produkovat error a zarovne rethrownut exception ?
            produceErrorEvent(ex);
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            // this signals the consumer thread that no more work is to be done
            consumerThread.shutdown();
        }

        // on a clean exit, wait for the runner thread
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            // may be the result of a wake-up interruption after an exception.
            // we ignore this here and only restore the interruption state
            interrupt();
        }

        produceEvent(WorkerEvent.WorkerState.FINISHED);
        log.info("S3Worker has finished");
    }

    @Override
    public void shutdown() {
        super.shutdown();
        handover.close();
        consumerThread.shutdown();
        interrupt(); // because it might be blocked by waiting to hand over data to collector
    }
}
