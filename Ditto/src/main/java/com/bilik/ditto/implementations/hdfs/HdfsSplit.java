package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.transfer.SourceSplit;
import com.bilik.ditto.core.util.Preconditions;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class HdfsSplit implements SourceSplit<List<Path>>, Iterable<Path> {

    private final List<Path> paths;

    public HdfsSplit(List<Path> paths) {
        Preconditions.requireNotEmpty(paths);
        this.paths = paths;
    }

    @Override
    public List<Path> getSplit() {
        return Collections.unmodifiableList(paths);
    }

    @Override
    public String toString() {
        String objNames =  paths.stream()
                .limit(Math.min(paths.size(), 10))
                .map(Path::toString)
                .collect(Collectors.joining("\n"))
                .toString();
        return "[S3Split:\n" + objNames + "\n, total paths: " + paths.size() + "]";
    }

    @Override
    public Iterator<Path> iterator() {
        return paths.iterator();
    }

    public int size() {
        return paths.size();
    }
}