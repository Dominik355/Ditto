package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.job.output.accumulation.FileUploader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsFileUploader implements FileUploader {

    private final Path parentPath;
    private final FileSystem fs;

    public HdfsFileUploader(Path parentPath, FileSystem fs) {
        this.parentPath = parentPath;
        this.fs = fs;
    }

    @Override
    public boolean upload(PathWrapper path) {
        try {
            fs.copyFromLocalFile(path.getHadoopPath(), new Path(parentPath, path.getNioPath().getFileName().toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        // not closing FS, because it is reused across the application. If caching of hadoop FS disabled, add close here
    }
}
