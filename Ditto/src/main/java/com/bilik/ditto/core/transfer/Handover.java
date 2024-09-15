package com.bilik.ditto.core.transfer;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.Serial;

import static com.bilik.ditto.core.util.Preconditions.checkNotNull;

/**
 * Handover is basically size 1 queue, which hands over data, so parsing and reading can be separated.
 * Does thread closing and waking up without Thread.interrupt().
 * This class is used to hand over data and exceptions.
 *
 * <p>The Handover has the notion of "waking up" the producer thread with a {@link WakeupException}
 * rather than a thread interrupt.
 *
 * <p>The Handover can also be "closed", signalling from one thread to the other that the thread
 * has terminated.
 */

public class Handover<T> implements Closeable {

    private final Object lock = new Object();

    private T next;
    private Exception error;
    private boolean wakeupProducer;

    /**
     * Polls the next element from the Handover, possibly blocking until the next element is available.
     * If an exception was handed in by the producer , then that exception is thrown
     * @return The next element (never null).
     * @throws ClosedException Thrown if the Handover was closed.
     * @throws Exception Rethrows exceptions.
     */
    @Nonnull
    public T pollNext() throws Exception {
        synchronized (lock) {
            while (next == null && error == null) {
                lock.wait();
            }

            T n = next;
            if (n != null) {
                next = null;
                lock.notifyAll();
                return n;
            } else {
                // if next is null, then thread has been notified because of error
                throw error;
            }
        }
    }

    /**
     * Hands over an element from the producer. If the Handover already has an element that was not
     * yet picked up by the consumer thread, this call blocks until the consumer picks up that
     * previous element.
     **
     * @throws WakeupException Thrown, if the wakeupProducer() method is called
     * @throws ClosedException Thrown if the Handover was closed
     */
    public void produce(final T element)
            throws InterruptedException, WakeupException, ClosedException {

        checkNotNull(element);

        synchronized (lock) {
            while (next != null && !wakeupProducer) {
                lock.wait();
            }

            wakeupProducer = false;

            // we should have been woken up only if element has been polled
            if (next != null) {
                throw new WakeupException();
            } else if (error == null) {
                // if there is no error, accept element and notify consumer
                next = element;
                lock.notifyAll();
            } else {
                // an error marks this as closed for the producer
                throw new ClosedException();
            }
        }
    }

    /**
     * Reports an exception. The consumer will throw the given exception immediately, if it is
     * currently blocked in the pollNext() method, or the next time it calls that method.
     */
    public void reportError(Exception t) {
        checkNotNull(t);

        synchronized (lock) {
            // do not override the initial exception
            if (error == null) {
                error = t;
            }
            next = null;
            lock.notifyAll();
        }
    }

    /**
     * Closes the handover. Both the sides will throw ClosedException
     */
    @Override
    public void close() {
        synchronized (lock) {
            next = null;
            wakeupProducer = false;

            if (error == null) {
                error = new ClosedException();
            }
            lock.notifyAll();
        }
    }

    /**
     * CCloses the handover. Both the sides will throw ClosedException.
     * This implmenetation does not erase current element, so consumer can finish it's work.
     */
    public void closeSoftly() {
        synchronized (lock) {
            wakeupProducer = false;

            if (error == null) {
                error = new ClosedException();
            }
            lock.notifyAll();
        }
    }

    /**
     * Wakes the producer thread up. If the producer thread is currently blocked
     * it will exit the method throwing a WakeupException
     */
    public void wakeupProducer() {
        synchronized (lock) {
            wakeupProducer = true;
            lock.notifyAll();
        }
    }

    // ------------------------------------------------------------------------

    /**
     * An exception thrown by the Handover in the pollNext() or produce()
     * method, after the Handover was closed.
     */
    public static final class ClosedException extends Exception {
        @Serial
        private static final long serialVersionUID = 1L;

        public ClosedException() {
            super("Handover is closed");
        }
    }

    /**
     * A special exception thrown bv the Handover in the produce() method
     * when the producer is woken up from a blocking call via wakeupProducer()
     */
    public static final class WakeupException extends Exception {
        @Serial
        private static final long serialVersionUID = 1L;
    }
}
