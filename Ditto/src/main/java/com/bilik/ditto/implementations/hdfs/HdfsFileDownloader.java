package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.util.WorkerPathProvider;
import org.apache.hadoop.fs.FileSystem;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class HdfsFileDownloader implements Closeable {

    private final WorkerPathProvider workerPathProvider;
    private final FileSystem fs;

    public HdfsFileDownloader(WorkerPathProvider workerPathProvider, FileSystem fs) {
        this.fs = fs;
        this.workerPathProvider = Objects.requireNonNull(workerPathProvider);

        try {
            Files.createDirectories(workerPathProvider.getParent().getNioPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path download(org.apache.hadoop.fs.Path path) {
        PathWrapper localPath = workerPathProvider.get();
        try {
            fs.copyToLocalFile(path, localPath.getHadoopPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return localPath.getNioPath();
    }

    @Override
    public void close() throws IOException {
        // not closing FS, because it is reused across the application. If caching of hadoop FS disabled, add close here
    }
}
