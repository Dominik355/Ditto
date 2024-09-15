package com.bilik.ditto.core.concurrent;

import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.testCommons.DummyWorkerThread;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ThreadFactoryTest {

    @Test
    void workerThreadModificationTest() {
        var exceptionHandler = new LoggingUncaughtExceptionHandler();
        var factory = new WorkerThreadFactory<>(
                "jobId",
                exceptionHandler,
                WorkerThread.WorkerType.SOURCE
        );

        DummyWorkerThread thread = new DummyWorkerThread();

        factory.modifyThread(thread, 15);

        assertThat(thread.isDaemon()).isTrue();
        assertThat(thread.getUncaughtExceptionHandler()).isEqualTo(exceptionHandler);
        assertThat(thread.getWorkerNumber()).isEqualTo(15);
        assertThat(thread.getName()).isEqualTo("jobId-source-15");
        assertThat(thread.getType()).isEqualTo(WorkerThread.WorkerType.SOURCE);
    }

    @Test
    void sinkProducerThreadFactoryTest() {
        var exceptionHandler = new LoggingUncaughtExceptionHandler();
        var factory = new SinkProducersThreadFactory(
                "jobId",
                exceptionHandler);

        Thread thread = factory.newThread(() -> {});

        assertThat(thread.isDaemon()).isTrue();
        assertThat(thread.getUncaughtExceptionHandler()).isEqualTo(exceptionHandler);
        assertThat(thread.getName()).isEqualTo("jobId-Sink_Producer-1");
    }

}
