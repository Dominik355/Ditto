package com.bilik.ditto.core.job.output.accumulation;

import com.bilik.ditto.core.job.StreamElement;

import java.io.Closeable;
import java.io.IOException;

public interface SinkAccumulator<IN> extends Closeable {

    /**
     * collect elements as they are coming
     */
    void collect(StreamElement<IN> element) throws InterruptedException, IOException;

    /**
     * check, if accumulator is full
     */
    boolean isFull();

    /**
     * if accumulator is full, get its result as runnable which
     * will be executed in ThreadPoolExecutor
     */
    Runnable onFull() throws Exception;

    Runnable onFinish() throws Exception;

    int size();

}