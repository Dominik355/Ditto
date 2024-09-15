package com.bilik.ditto.core.job.output;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.job.output.accumulation.DummyAccumulator;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import com.bilik.ditto.testCommons.SensorGenerator;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

public class SinkWorkerTest {

    @Test
    void defaultTest() {
        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        BlockingQueue<StreamElement<Sensor>> sourceQueue = new LinkedBlockingQueue<>();
        DummyAccumulator<Sensor> sinkAccumulator = new DummyAccumulator<>(5);

        SinkWorker<Sensor> workerThread = new SinkWorker<>(
                workerEventProducer,
                sourceQueue,
                sinkAccumulator,
                Executors.newSingleThreadExecutor()
        );

        SensorGenerator.getRange(0, 7).stream()
                .map(StreamElement::of)
                .forEach(sourceQueue::add);
        sourceQueue.add(StreamElement.last());

        workerThread.initThread();
        workerThread.run();

        assertThat(workerEventProducer.events()).hasSize(2);
        assertThat(workerEventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
        assertThat(workerEventProducer.events().get(1).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.FINISHED);

        assertThat(sinkAccumulator.getFullCounter()).isEqualTo(2);
        assertThat(sinkAccumulator.getTotal()).isEqualTo(7);
    }

    @Test
    void notEnoughElementsToFullAccumulator() {
        MockingWorkerEventProducer workerEventProducer = new MockingWorkerEventProducer();
        BlockingQueue<StreamElement<Sensor>> sourceQueue = new LinkedBlockingQueue<>();
        DummyAccumulator<Sensor> sinkAccumulator = new DummyAccumulator<>(50);

        SinkWorker<Sensor> workerThread = new SinkWorker<>(
                workerEventProducer,
                sourceQueue,
                sinkAccumulator,
                Executors.newSingleThreadExecutor()
        );

        SensorGenerator.getRange(0, 7).stream()
                .map(StreamElement::of)
                .forEach(sourceQueue::add);
        sourceQueue.add(StreamElement.last());

        workerThread.initThread();
        workerThread.run();

        assertThat(workerEventProducer.events()).hasSize(2);
        assertThat(workerEventProducer.events().get(0).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);
        assertThat(workerEventProducer.events().get(1).getWorkerState()).isEqualTo(WorkerEvent.WorkerState.FINISHED);

        assertThat(sinkAccumulator.getFullCounter()).isEqualTo(1);
        assertThat(sinkAccumulator.getTotal()).isEqualTo(7);
    }

}
