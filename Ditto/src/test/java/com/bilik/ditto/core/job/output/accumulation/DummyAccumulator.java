package com.bilik.ditto.core.job.output.accumulation;

import com.bilik.ditto.core.job.StreamElement;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class DummyAccumulator<T> implements SinkAccumulator<T> {

    private final List<StreamElement<T>> elements = new LinkedList<>();
    private final int capacity;
    private int fullCounter;
    private int total;

    public DummyAccumulator(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public void collect(StreamElement<T> element) throws InterruptedException, IOException {
        elements.add(element);
        total++;
    }

    @Override
    public boolean isFull() {
        return elements.size() >= capacity;
    }

    @Override
    public Runnable onFull() throws Exception {
        fullCounter++;
        elements.clear();
        return () -> {};
    }

    @Override
    public Runnable onFinish() throws Exception {
        return onFull();
    }

    @Override
    public int size() {
        return elements.size();
    }

    public List<StreamElement<T>> getElements() {
        return elements;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getFullCounter() {
        return fullCounter;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public void close() throws IOException {
    }
}
