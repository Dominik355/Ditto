package com.bilik.ditto.core.transfer;

public interface SourceSplit<T> {

    T getSplit();

    int size();
}
