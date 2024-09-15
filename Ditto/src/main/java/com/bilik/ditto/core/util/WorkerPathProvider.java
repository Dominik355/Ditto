package com.bilik.ditto.core.util;

import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.io.PathWrapper;

import java.nio.file.Path;
import java.util.function.Supplier;

public class WorkerPathProvider implements Supplier<PathWrapper> {

    protected long count;
    private final String jobId;
    private final Path parentPath;
    private final String fileExtension;
    private final int workerId;
    private final WorkerThread.WorkerType type;

    public WorkerPathProvider(String jobId,
                              Path parentPath,
                              String fileExtension,
                              int workerId,
                              WorkerThread.WorkerType type) {
        this.jobId = jobId;
        this.parentPath = parentPath;
        this.fileExtension = fileExtension;
        this.workerId = workerId;
        this.type = type;
    }

    /**
     * So we can create parent directories
     */
    public PathWrapper getParent() {
        String newFile = String.format("%s/%s/%d",
                jobId,
                type.name,
                workerId);
        return new PathWrapper(parentPath.resolve(newFile));
    }

    @Override
    public PathWrapper get() {
        String newFile = String.format("%s/%s/%d/%s-%d-%d.%s",
                jobId,
                type.name,
                workerId,
                jobId,
                workerId,
                count++,
                fileExtension);
        return new PathWrapper(parentPath.resolve(newFile));
    }

}
