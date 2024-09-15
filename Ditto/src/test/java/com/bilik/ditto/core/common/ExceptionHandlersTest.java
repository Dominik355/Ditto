package com.bilik.ditto.core.common;

import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.testCommons.MockingWorkerEventProducer;
import com.bilik.ditto.testCommons.StoppableClass;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExceptionHandlersTest {

    @Test
    void testStoppingHandlerWrappedInLogging() {
        var stoppableClass = new StoppableClass();
        var exceptionHandler = LoggingUncaughtExceptionHandler.withChild(
                new StoppingUncaughtExceptionHandler(stoppableClass));

        exceptionHandler.uncaughtException(Thread.currentThread(), new DittoRuntimeException("Testing exception"));

        Assertions.assertThat(stoppableClass.hasBeenStopped()).isTrue();
    }

    @Test
    void testErrorProducingHandlerWrappedInLogging() {
        var producer = new MockingWorkerEventProducer();
        var exceptionHandler = LoggingUncaughtExceptionHandler.withChild(
                new ErrorEventProducingUncaughtExceptionHandler(producer));

        exceptionHandler.uncaughtException(Thread.currentThread(), new DittoRuntimeException("Testing exception"));

        Assertions.assertThat(producer.events()).hasSize(1);
    }

}
