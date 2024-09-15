package com.bilik.ditto.core.io;

import java.nio.file.Path;
import java.util.Objects;

public class PathWrapper {

    private final Path path;

    public PathWrapper(Path path) {
        this.path = Objects.requireNonNull(path);
    }

    public Path getNioPath() {
        return this.path;
    }

    public org.apache.hadoop.fs.Path getHadoopPath() {
        return new org.apache.hadoop.fs.Path(this.path.toUri());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PathWrapper that = (PathWrapper) o;
        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public String toString() {
        return "PathWrapper[" + path.toString() + "]";
    }

}
