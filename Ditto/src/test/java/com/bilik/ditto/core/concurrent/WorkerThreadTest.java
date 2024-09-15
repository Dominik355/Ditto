package com.bilik.ditto.core.concurrent;

import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.core.concurrent.WorkerThread.WorkerType;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent.WorkerState;
import com.bilik.ditto.testCommons.DummyWorkerThread;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import com.bilik.ditto.testCommons.ThreadUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class WorkerThreadTest {

    @Test
    void defaultTest() throws InterruptedException, TimeoutException {
        // given
        final WorkerType type = WorkerType.SINK;
        final int num = 321;
        final MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();
        final DummyWorkerThread workerThread = new DummyWorkerThread(eventProducer);

        // when - then
        workerThread.setType(type);
        assertThat(workerThread.getType()).isEqualTo(type);
        
        workerThread.setWorkerNumber(num);
        assertThat(workerThread.getWorkerNumber()).isEqualTo(num);

        assertThat(workerThread.initThread()).isTrue();
        assertThat(workerThread.isInitialized()).isTrue();
        
        workerThread.produceEvent(WorkerState.ERROR);
        assertThat(eventProducer.events()).hasSize(1);

        WorkerEvent produced = eventProducer.events().get(0);
        assertThat(produced.getType()).isEqualTo(type);
        assertThat(produced.getThreadNum()).isEqualTo(num);
        assertThat(produced.getWorkerState()).isEqualTo(WorkerState.ERROR);

        // copy
        final DummyWorkerThread workerThreadChild = new DummyWorkerThread(eventProducer);
        workerThread.copyProperties(workerThreadChild, "testSuffix");
        assertThat(workerThreadChild.getWorkerNumber()).isEqualTo(num);
        assertThat(workerThreadChild.getName()).isEqualTo(workerThread.getName() + "-testSuffix");
        assertThat(workerThreadChild.getType()).isEqualTo(type);
        assertThat(workerThreadChild.isDaemon()).isEqualTo(workerThread.isDaemon());
        assertThat(workerThreadChild.getUncaughtExceptionHandler()).isEqualTo(workerThread.getUncaughtExceptionHandler());

        // run
        assertThat(workerThread.isRunning()).isFalse();

        workerThread.start();
        ThreadUtils.waitWhile(() -> eventProducer.events().size() != 2, 5_000);
        assertThat(workerThread.isRunning()).isTrue();

        workerThread.shutdown();
        workerThread.join(5_000);
        assertThat(workerThread.isRunning()).isFalse();
    }

    @Test
    void testWithFactory() throws InterruptedException, TimeoutException {
        // given
        final int num = 15;
        final WorkerType type = WorkerType.SOURCE;
        final MockingWorkerEventProducer eventProducer = new MockingWorkerEventProducer();

        WorkerThreadFactory<DummyWorkerThread> factory = new WorkerThreadFactory<>(
                "jobId",
                new LoggingUncaughtExceptionHandler(),
                type
        );

        DummyWorkerThread workerThread = factory.modifyThread(new DummyWorkerThread(eventProducer), 15);

        // when - then
        workerThread.initThread();
        assertThat(workerThread.isInitialized()).isTrue();

        workerThread.produceEvent(WorkerState.ERROR);
        assertThat(eventProducer.events()).hasSize(1);

        WorkerEvent produced = eventProducer.events().get(0);
        assertThat(produced.getType()).isEqualTo(type);
        assertThat(produced.getThreadNum()).isEqualTo(num);
        assertThat(produced.getWorkerState()).isEqualTo(WorkerState.ERROR);

        // copy
        final DummyWorkerThread workerThreadChild = new DummyWorkerThread(eventProducer);
        workerThread.copyProperties(workerThreadChild, "testSuffix");
        assertThat(workerThreadChild.getWorkerNumber()).isEqualTo(num);
        assertThat(workerThreadChild.getName()).isEqualTo(workerThread.getName() + "-testSuffix");
        assertThat(workerThreadChild.getType()).isEqualTo(type);
        assertThat(workerThreadChild.isDaemon()).isEqualTo(workerThread.isDaemon());
        assertThat(workerThreadChild.getUncaughtExceptionHandler()).isEqualTo(workerThread.getUncaughtExceptionHandler());

        // run
        assertThat(workerThread.isRunning()).isFalse();

        workerThread.start();
        ThreadUtils.waitWhile(() -> eventProducer.events().size() != 2, 5_000);
        assertThat(workerThread.isRunning()).isTrue();

        workerThread.shutdown();
        workerThread.join(5_000);
        assertThat(workerThread.isRunning()).isFalse();
    }

}
