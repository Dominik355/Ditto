package com.bilik.ditto.core.job;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public interface Collector<T> {

    /**
     * Instead of deserializing elements one by one and then sending them to Queue,
     * We use this approach, because when our input is batch of data (like file),
     * we need to iterate over all records until that batch is empty.
     * -----
     * This approach also works, because in our handover-architecture of workers,
     * we should be always handing over batches of data. So when actual deserializing takes a place,
     * we know we have batch of data even when those are separate elements (like from Kafka).
     */
    void collect(StreamElement<T> streamElement) throws InterruptedException;



    /**
     * Closes the collector. If any data was buffered, that data will be flushed.
     */
    void close();

    class QueueCollector<T> implements Collector<T> {

        private final BlockingQueue<StreamElement<T>> queue;

        public QueueCollector(BlockingQueue<StreamElement<T>> queue) {
            this.queue = queue;
        }

        /**
         * we want to offer those elements for sure, that's why that timeout.
         * Only thing that can change it is error, which should cause thread interruption
         */
        @Override
        public void collect(StreamElement<T> streamElement) throws InterruptedException {
            queue.offer(streamElement, Long.MAX_VALUE, TimeUnit.SECONDS);
        }

        @Override
        public void close() {

        }
    }
}