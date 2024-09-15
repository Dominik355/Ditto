package com.bilik.ditto.core.metric;

public interface Counter {

    void inc();

    void inc(long n);

    void set(long n);

    void dec();

    void dec(long n);

    long getCount();

}