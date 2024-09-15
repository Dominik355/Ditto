package com.bilik.ditto.testCommons;

import com.bilik.ditto.core.io.FileReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.IntStream;

public class DummyFileReader implements FileReader<byte[]> {

    private final Iterator<byte[]> iterator;

    public DummyFileReader(int count) {
        this.iterator = IntStream.range(0, count)
                .mapToObj(i -> new byte[i])
                .iterator();
    }

    @Override
    public byte[] next() throws IOException {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
