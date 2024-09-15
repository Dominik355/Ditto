package com.bilik.ditto.testCommons;

import com.bilik.ditto.core.concurrent.WorkerThread;
import org.assertj.core.api.Assertions;

import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ThreadUtils {

    public static void waitWhile(Supplier<Boolean> supplier, long millis) throws InterruptedException, TimeoutException {
        long waiting = 0;
        while(supplier.get()) {
            Thread.sleep(10);
            waiting += 10;
            if (waiting > millis) {
                throw new TimeoutException("Thread " + Thread.currentThread().getName() + " has been waiting for more than " + millis + " millis");
            }
        }
    }

    public static void assertThreadTermination(Thread t) throws InterruptedException {
        t.join(5_000);
        if (t.isAlive()) {
            throw new AssertionError("Thread " + t + " has not been terminated within 5 seconds");
        }
    }

    public static void assertNoWorkerThreadsAlive() {
        Set<WorkerThread> workers = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t instanceof WorkerThread)
                .map(t -> (WorkerThread) t)
                .collect(Collectors.toSet());
        Assertions.assertThat(workers).isEmpty();
    }

    public static class TestingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        private final Object lock = new Object();

        private Throwable exception;

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            synchronized (lock) {
                this.exception = e;
                lock.notifyAll();
            }
        }

        public Throwable waitForException() throws InterruptedException {
            synchronized (lock) {
                if (exception == null) {
                    lock.wait(5_000);
                }
                return exception;
            }
        }

        public Throwable getException() {
            return exception;
        }
    }

}
