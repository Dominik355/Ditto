package com.bilik.ditto.core.metric;

/**
 * Not threadsafe implementation
 */
public class SimpleCounter implements Counter {

    private long count;

    @Override
    public void inc() {
        count++;
    }

    @Override
    public void inc(long n) {
        count += n;
    }

    @Override
    public void set(long n) {
        count = n;
    }

    @Override
    public void dec() {
        count--;
    }

    @Override
    public void dec(long n) {
        count -= n;
    }

    @Override
    public long getCount() {
        return count;
    }

}
