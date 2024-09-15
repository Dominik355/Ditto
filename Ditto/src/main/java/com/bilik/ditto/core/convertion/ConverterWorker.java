package com.bilik.ditto.core.convertion;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.job.StreamElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class ConverterWorker<IN, OUT> extends WorkerThread {

    private final static Logger log = LoggerFactory.getLogger(ConverterWorker.class);

    private final BlockingQueue<StreamElement<IN>> input;
    private final BlockingQueue<StreamElement<OUT>> output;
    private final Converter<IN, OUT> converter;

    public ConverterWorker(WorkerEventProducer eventProducer,
                           BlockingQueue<StreamElement<IN>> input,
                           BlockingQueue<StreamElement<OUT>> output,
                           Converter<IN, OUT> converter) {
        super(eventProducer);
        this.input = requireNonNull(input);
        this.output = requireNonNull(output);
        this.converter = converter;
    }

    @Override
    public void run() {
        if (running) {
            log.warn("ConverterWorker [{}] is already in running state", getName());
            return;
        } else {
            running = true;
            produceEvent(WorkerEvent.WorkerState.STARTED);
            log.info("ConverterWorker[{}] has started", getName());
        }

        try {
            while(running) {
                StreamElement<IN> element = input.poll(Long.MAX_VALUE, TimeUnit.SECONDS);
                if (element != null) { // if null then worker has been interrupted
                    if (element.isLast()) {
                        log.info("Source has reached last element. Sending it further and breaking out of while to finish thread.");
                        output.offer(converter.convertLastElement(element));
                        break;
                    }
                    OUT out = converter.convert(element.value());
                    output.offer(StreamElement.of(out, element), Long.MAX_VALUE, TimeUnit.SECONDS);
                }
            }
        } catch (InterruptedException e) {
            log.info("ConverterWorker has been interrupted");
        } catch (Exception e) {
            log.error("Exception occurred in ConverterWorker. Most probable reason is conversion using converter {}", converter.getClass());
            produceErrorEvent(e);
            throw new RuntimeException(e);
        }

        produceEvent(WorkerEvent.WorkerState.FINISHED);
        log.info("ConverterWorker[{}] has finished", getName());
    }

    /**
     * Everything is being supplied already, because some of the converters need user input
     * and can not be automatically resolved;
     */
    @Override
    public boolean initThread() {
        return true;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        interrupt();
    }
}
