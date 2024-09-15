package com.bilik.ditto.implementations.local;

import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.job.output.accumulation.FileUploader;
import com.bilik.ditto.core.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;

public class LocalFileUploader implements FileUploader {

    private final Path dirPath;

    public LocalFileUploader(Path dirPath) {
        this.dirPath = dirPath;
    }

    @Override
    public boolean upload(PathWrapper path) {
        try {
            FileUtils.moveFile(path.getNioPath(), dirPath.resolve(path.getNioPath().getFileName()));
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
