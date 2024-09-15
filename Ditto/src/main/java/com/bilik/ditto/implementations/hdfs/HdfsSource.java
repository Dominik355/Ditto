package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.core.common.StoppingUncaughtExceptionHandler;
import com.bilik.ditto.core.exception.InvalidUserInputException;
import com.bilik.ditto.core.util.CollectionUtils;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.WorkerThreadFactory;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.core.util.WorkerPathProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class HdfsSource<T> extends Source<T> {

    private static final Logger log = LoggerFactory.getLogger(HdfsSource.class);

    private final HdfsFileRange fileRange;

    private final FileRWFactory<T> fileRWFactory;

    private final FileSystem fileSystem;

    private long pathsTotal;

    public HdfsSource(int parallelism,
                      Type<T> type,
                      HdfsFileRange fileRange,
                      String jobId,
                      WorkerEventProducer eventProducer,
                      FileRWFactory<T> fileRWFactory,
                      FileSystem fileSystem) {
        super(parallelism, type, jobId, eventProducer);
        this.fileRange = fileRange;
        this.fileRWFactory = fileRWFactory;
        this.fileSystem = fileSystem;
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        // not closing FS, because it is reused across the application. If caching of hadoop FS disabled, add close here
    }

    @Override
    public Collection<Integer> initialize(Map<Integer, BlockingQueue<StreamElement<T>>> workerMap) {
        WorkerThreadFactory<HdfsWorker<T>> threadFactory =
                new WorkerThreadFactory<>(jobId,
                        LoggingUncaughtExceptionHandler.withChild(
                                new StoppingUncaughtExceptionHandler(this)),
                        WorkerThread.WorkerType.SOURCE);

        workerThreads = new HashMap<>();

        List<Path> allPaths = new LinkedList<>();

        try {
            if (!fileSystem.exists(fileRange.getParentPath())) {
                throw InvalidUserInputException.of("Path {} does not exist in defined HDFS cluster", fileRange.getStringParentPath());
            }

            if (fileSystem.getFileStatus(fileRange.getParentPath()).isFile()) {
                log.info("Defined parent path [{}] is actually a single file. Changing parallelism to 1 and processing only this single file.", fileRange.getStringParentPath());
                allPaths.add(fileRange.getParentPath());
            } else {
                RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(fileRange.getParentPath(), fileRange.isRecursive());
                LocatedFileStatus current;
                while (iterator.hasNext()) {
                    current = iterator.next();
                    if (fileRange.fulfillsPredicates(current)) {
                        allPaths.add(current.getPath());
                    }
                }
                log.info("{} files has been found", allPaths.size());

                if (allPaths.isEmpty()) {
                    throw DittoRuntimeException.of("No file for criteria [{}] has been found", fileRange);
                }
            }

            if (allPaths.size() < parallelism) {
                log.info("Expected parallelism [{}] can not be fulfilled, because we did not found enough files [{}]. So new parallelism is {}", parallelism, allPaths.size(), allPaths.size());
                this.parallelism = allPaths.size();
            }

            pathsTotal = allPaths.size();
            List<List<Path>> splitted = CollectionUtils.toNParts(allPaths, parallelism);
            List<HdfsSplit> splits = splitted.stream()
                    .map(HdfsSplit::new)
                    .toList();

            for (int split = 1; split <= parallelism; split++) {
                log.info("Initializing HDFS worker {}/{}, which has assigned {} files", split, parallelism, splits.get(split - 1).size());
                HdfsWorker<T> workerThread = threadFactory.modifyThread(new HdfsWorker<>(
                        eventProducer,
                        fileRWFactory,
                        workerMap.get(split),
                        counterAggregator,
                        splits.get(split - 1),
                        new HdfsFileDownloader(
                                new WorkerPathProvider(
                                        jobId,
                                        fileRWFactory.getInfo().parentPath(),
                                        fileRWFactory.getInfo().fileExtension(),
                                        split,
                                        WorkerThread.WorkerType.SOURCE),
                                fileSystem)
                ), split);

                boolean initialized = workerThread.initThread();
                if(!initialized) {
                    continue;
                }
                workerThreads.put(workerThread.getWorkerNumber(), workerThread);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return workerThreads.keySet();
    }

    public long getPathsTotal() {
        return pathsTotal;
    }

    public static class HdfsSourceBuilder<T> extends Source.SourceBuilder<T, HdfsSourceBuilder<T>> {
        private HdfsFileRange fileRange;
        private FileRWFactory<T> fileRWFactory;
        private FileSystem fileSystem;

        public HdfsSourceBuilder<T> fileRange(HdfsFileRange fileRange) {
            this.fileRange = fileRange;
            return this;
        }

        public HdfsSourceBuilder<T> fileRWFactory(FileRWFactory<T> fileRWFactory) {
            this.fileRWFactory = fileRWFactory;
            return this;
        }

        public HdfsSourceBuilder<T> fileSystem(FileSystem fileSystem) {
            this.fileSystem = fileSystem;
            return this;
        }

        @Override
        public HdfsSource<T> build() {
            return new HdfsSource<>(parallelism, type, fileRange, jobId, eventProducer, fileRWFactory, fileSystem);
        }

        @Override
        protected HdfsSourceBuilder<T> self() {
            return this;
        }
    }

}
