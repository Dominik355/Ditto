package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.transfer.SourceSplit;
import com.bilik.ditto.core.util.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class S3Split implements SourceSplit<List<String>>, Iterable<String> {

    private final List<String> objects;

    public S3Split(List<String> objects) {
        Preconditions.requireNotEmpty(objects);
        this.objects = objects;
    }

    @Override
    public List<String> getSplit() {
        return Collections.unmodifiableList(objects);
    }

    @Override
    public String toString() {
        int range = Math.min(objects.size(), 10);
        String objNames =  IntStream.range(0, range)
                .mapToObj(objects::get)
                .collect(Collectors.joining("\n"))
                .toString();
        return "[S3Split:\n" + objNames + "\n, total objects: " + objects.size() + "]";
    }

    @Override
    public Iterator<String> iterator() {
        return objects.iterator();
    }

    public int size() {
        return objects.size();
    }
}
