package com.bilik.ditto.core.metric;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class ThreadSafeCounter implements Counter {

    private final AtomicLong count = new AtomicLong();

    @Override
    public void inc() {
        count.incrementAndGet();
    }

    @Override
    public void inc(long n) {
        count.addAndGet(n);
    }

    @Override
    public void set(long n) {
        count.set(n);
    }

    @Override
    public void dec() {
        count.decrementAndGet();
    }

    @Override
    public void dec(long n) {
        count.getAndAdd(-n);
    }

    @Override
    public long getCount() {
        return count.get();
    }
}
