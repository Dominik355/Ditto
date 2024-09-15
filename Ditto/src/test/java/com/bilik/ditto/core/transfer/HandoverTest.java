package com.bilik.ditto.core.transfer;

import com.bilik.ditto.core.util.ExceptionUtils;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class HandoverTest {

    // ------------------------------------------------------------------------
    //  test produce / consumer
    // ------------------------------------------------------------------------

    @Test
    public void testWithVariableProducer() throws Exception {
        runProducerConsumerTest(500, 2, 0);
    }

    @Test
    public void testWithVariableConsumer() throws Exception {
        runProducerConsumerTest(500, 0, 2);
    }

    @Test
    public void testWithVariableBoth() throws Exception {
        runProducerConsumerTest(500, 2, 2);
    }

    // ------------------------------------------------------------------------
    //  test error propagation
    // ------------------------------------------------------------------------

    @Test
    public void testPublishErrorOnEmptyHandover() throws Exception {
        final Handover handover = new Handover();

        Exception error = new Exception();
        handover.reportError(error);

        try {
            handover.pollNext();
            fail("should throw an exception");
        }
        catch (Exception e) {
            assertEquals(error, e);
        }
    }

    @Test
    public void testPublishErrorOnFullHandover() throws Exception {
        final Handover handover = new Handover();
        handover.produce(createTestRecord());

        IOException error = new IOException();
        handover.reportError(error);

        try {
            handover.pollNext();
            fail("should throw an exception");
        }
        catch (Exception e) {
            assertEquals(error, e);
        }
    }

    @Test
    public void testExceptionMarksClosedOnEmpty() throws Exception {
        final Handover handover = new Handover();

        IllegalStateException error = new IllegalStateException();
        handover.reportError(error);

        try {
            handover.produce(createTestRecord());
            fail("should throw an exception");
        }
        catch (Handover.ClosedException e) {
            // expected
        }
    }

    @Test
    public void testExceptionMarksClosedOnFull() throws Exception {
        final Handover handover = new Handover();
        handover.produce(createTestRecord());

        Exception error = new Exception();
        handover.reportError(error);

        try {
            handover.produce(createTestRecord());
            fail("should throw an exception");
        }
        catch (Handover.ClosedException e) {
            // expected
        }
    }

    // ------------------------------------------------------------------------
    //  test closing behavior
    // ------------------------------------------------------------------------

    @Test
    public void testCloseEmptyForConsumer() throws Exception {
        final Handover handover = new Handover();
        handover.close();

        try {
            handover.pollNext();
            fail("should throw an exception");
        }
        catch (Handover.ClosedException e) {
            // expected
        }
    }

    @Test
    public void testCloseFullForConsumer() throws Exception {
        final Handover handover = new Handover();
        handover.produce(createTestRecord());
        handover.close();

        try {
            handover.pollNext();
            fail("should throw an exception");
        }
        catch (Handover.ClosedException e) {
            // expected
        }
    }

    @Test
    public void testCloseEmptyForProducer() throws Exception {
        final Handover handover = new Handover();
        handover.close();

        try {
            handover.produce(createTestRecord());
            fail("should throw an exception");
        }
        catch (Handover.ClosedException e) {
            // expected
        }
    }

    @Test
    public void testCloseFullForProducer() throws Exception {
        final Handover handover = new Handover();
        handover.produce(createTestRecord());
        handover.close();

        try {
            handover.produce(createTestRecord());
            fail("should throw an exception");
        }
        catch (Handover.ClosedException e) {
            // expected
        }
    }

    // ------------------------------------------------------------------------
    //  test wake up behavior
    // ------------------------------------------------------------------------

    @Test
    public void testWakeupDoesNotWakeWhenEmpty() throws Exception {
        Handover handover = new Handover();
        handover.wakeupProducer();

        // produce into a woken but empty handover
        try {
            handover.produce(createTestRecord());
        }
        catch (Handover.WakeupException e) {
            fail();
        }

        // handover now has records, next time we wakeup and produce it needs
        // to throw an exception
        handover.wakeupProducer();
        try {
            handover.produce(createTestRecord());
            fail("should throw an exception");
        }
        catch (Handover.WakeupException e) {
            // expected
        }

        // empty the handover
        assertNotNull(handover.pollNext());

        // producing into an empty handover should work
        try {
            handover.produce(createTestRecord());
        }
        catch (Handover.WakeupException e) {
            fail();
        }
    }

    @Test
    public void testWakeupWakesOnlyOnce() throws Exception {
        // create a full handover
        final Handover handover = new Handover();
        handover.produce(createTestRecord());

        handover.wakeupProducer();

        try {
            handover.produce(createTestRecord());
            fail();
        } catch (Handover.WakeupException e) {
            // expected
        }

        CheckedThread producer = new CheckedThread() {
            @Override
            public void go() throws Exception {
                handover.produce(createTestRecord());
            }
        };
        producer.start();

        // the producer must go blocking
        producer.waitUntilThreadHoldsLock(10000);

        // release the thread by consuming something
        assertNotNull(handover.pollNext());
        producer.sync();
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private void runProducerConsumerTest(int numRecords, int maxProducerDelay, int maxConsumerDelay) throws Exception {
        // generate test data
        @SuppressWarnings({"unchecked", "rawtypes"})
        final List<Sensor> data = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            data.add(i, createTestRecord());
        }

        final Handover handover = new Handover();

        ProducerThread producer = new ProducerThread(handover, data, maxProducerDelay);
        ConsumerThread consumer = new ConsumerThread(handover, data, maxConsumerDelay);

        consumer.start();
        producer.start();

        // sync first on the consumer, so it propagates assertion errors
        consumer.sync();
        producer.sync();
    }

    @SuppressWarnings("unchecked")
    private static Sensor createTestRecord() {
        return mock(Sensor.class);
    }

    // ------------------------------------------------------------------------

    private abstract static class CheckedThread extends Thread {

        private volatile Throwable error;

        public abstract void go() throws Exception;

        @Override
        public void run() {
            try {
                go();
            }
            catch (Throwable t) {
                error = t;
            }
        }

        public void sync() throws Exception {
            join();
            if (error != null) {
                ExceptionUtils.rethrowException(error, error.getMessage());
            }
        }

        public void waitUntilThreadHoldsLock(long timeoutMillis) throws InterruptedException, TimeoutException {
            final long deadline = System.nanoTime() + timeoutMillis * 1_000_000;

            while (!isBlockedOrWaiting() && (System.nanoTime() < deadline)) {
                Thread.sleep(1);
            }

            if (!isBlockedOrWaiting()) {
                throw new TimeoutException();
            }
        }

        private boolean isBlockedOrWaiting() {
            State state = getState();
            return state == State.BLOCKED || state == State.WAITING || state == State.TIMED_WAITING;
        }
    }

    private static class ProducerThread extends CheckedThread {

        private final Random rnd = new Random();
        private final Handover<Sensor> handover;
        private final List<Sensor> data;
        private final int maxDelay;

        private ProducerThread(Handover<Sensor>handover, List<Sensor> data, int maxDelay) {
            this.handover = handover;
            this.data = data;
            this.maxDelay = maxDelay;
        }

        @Override
        public void go() throws Exception {
            for (Sensor rec : data) {
                handover.produce(rec);

                if (maxDelay > 0) {
                    int delay = rnd.nextInt(maxDelay);
                    Thread.sleep(delay);
                }
            }
        }
    }

    private static class ConsumerThread extends CheckedThread {

        private final Random rnd = new Random();
        private final Handover<Sensor> handover;
        private final List<Sensor> data;
        private final int maxDelay;

        private ConsumerThread(Handover<Sensor> handover, List<Sensor> data, int maxDelay) {
            this.handover = handover;
            this.data = data;
            this.maxDelay = maxDelay;
        }

        @Override
        public void go() throws Exception {
            for (Sensor rec : data) {
                Sensor next = handover.pollNext();

                assertEquals(rec, next);

                if (maxDelay > 0) {
                    int delay = rnd.nextInt(maxDelay);
                    Thread.sleep(delay);
                }
            }
        }
    }

}
