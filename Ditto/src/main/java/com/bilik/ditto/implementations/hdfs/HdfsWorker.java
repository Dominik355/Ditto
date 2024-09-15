package com.bilik.ditto.implementations.hdfs;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class HdfsWorker<T> extends WorkerThread {

    private static final Logger log = LoggerFactory.getLogger(WorkerThread.class);

    private final FileRWFactory<T> fileRWFactory;

    private final BlockingQueue<StreamElement<T>> sinkQueue;

    private final CounterAggregator counterAggregator;

    private final Handover<PathWrapper> handover;

    private final HdfsSplit split;

    private final HdfsFileDownloader downloader;

    private HdfsConsumerThread consumerThread;

    private Counter readRecords;
    private Counter readFiles;

    protected HdfsWorker(WorkerEventProducer workerEventProducer,
                         FileRWFactory<T> fileRWFactory,
                         BlockingQueue<StreamElement<T>> sinkQueue,
                         CounterAggregator counterAggregator,
                         HdfsSplit split,
                         HdfsFileDownloader downloader) {
        super(workerEventProducer);
        this.fileRWFactory = fileRWFactory;
        this.sinkQueue = sinkQueue;
        this.counterAggregator = counterAggregator;
        this.split = split;
        this.downloader = downloader;

        this.handover = new Handover<>();
    }

    @Override
    public boolean initThread() {
        super.initThread();
        this.readFiles = counterAggregator.addCounter(getName() + "_read_files", new ThreadSafeCounter());
        this.readRecords = counterAggregator.addCounter(getName() + "_read_records", new ThreadSafeCounter());
        this.consumerThread = new HdfsConsumerThread(
                workerEventProducer,
                downloader,
                handover,
                split,
                counterAggregator
        );
        log.info("Initialized HdfsConsumerThread");

        // because we didn't use factory to create consumer thread, we have to set it this way
        copyProperties(this.consumerThread, "consumer");
        return this.consumerThread.initThread();
    }

    @Override
    public void run() {
        if (running) {
            log.warn("HdfsWorker [{}] is already in running state", getName());
            return;
        } else if (!hasBeenInitialized) {
            throw new DittoRuntimeException("WorkerThread has not been initialized yet!");
        } else {
            running = true;
            produceEvent(WorkerEvent.WorkerState.STARTED);
            log.info("HdfsWorker is running");
        }

        try {
            // start actual consumer
            consumerThread.start();

            Collector<T> collector = new Collector.QueueCollector<>(sinkQueue);

            while (running) {
                // blocks until we get the next records
                // it automatically re-throws exceptions encountered in the consumer thread
                PathWrapper pathWrapper = handover.pollNext();
                readFiles.inc();

                try (FileReader<T> reader = fileRWFactory.buildFileReader(pathWrapper)) {
                    T currentElement;
                    while ((currentElement = reader.next()) != null) {
                        collector.collect(StreamElement.of(currentElement));
                        readRecords.inc();
                    }
                } catch (Exception ex) {
                    log.error("Error occurred while reading file {}", pathWrapper);
                    throw ex;
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
        log.info("HdfsWorker has finished");
    }

    @Override
    public void shutdown() {
        super.shutdown();
        handover.close();
        consumerThread.shutdown();
        interrupt(); // because it might be blocked by waiting to hand over data to collector
    }

}
