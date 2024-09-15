package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.core.common.StoppingUncaughtExceptionHandler;
import com.bilik.ditto.core.util.CollectionUtils;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.WorkerThreadFactory;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.util.WorkerPathProvider;
import com.bilik.ditto.core.job.StreamElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class S3Source<T> extends Source<T> {

    private static final Logger log = LoggerFactory.getLogger(S3Source.class);

    private final S3ObjectRange s3ObjectRange;

    private final String bucket;

    private final S3Handler s3Handler;

    private final FileRWFactory<T> fileRWFactory;

    private long objectsTotal;

    public S3Source(int parallelism,
                    Type<T> type,
                    S3ObjectRange s3ObjectRange,
                    String jobId,
                    String bucket,
                    S3Handler s3Handler,
                    WorkerEventProducer eventProducer,
                    FileRWFactory<T> fileRWFactory) {
        super(parallelism, type, jobId, eventProducer);
        this.s3ObjectRange = s3ObjectRange;
        this.bucket = bucket;
        this.s3Handler = s3Handler;
        this.fileRWFactory = fileRWFactory;
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        s3Handler.close(); // s3handler is threadsafe and shared between all workers. So close it only when everyone is finished
    }

    @Override
    public Collection<Integer> initialize(Map<Integer, BlockingQueue<StreamElement<T>>> workerMap) {
        WorkerThreadFactory<S3Worker<T>> threadFactory =
                new WorkerThreadFactory<>(jobId,
                        LoggingUncaughtExceptionHandler.withChild(
                                new StoppingUncaughtExceptionHandler(this)),
                        WorkerThread.WorkerType.SOURCE);

        workerThreads = new HashMap<>();

        List<String> allObjects = new LinkedList<>();

        // TODO : async call
        for (String prefix : s3ObjectRange.getPrefixes()) {
            var iterator = s3Handler.listObjectsIterator(bucket, prefix);
            List<String> found = new LinkedList<>();

            while (iterator.hasNext()) {
                for (S3Object object : iterator.next().contents()) {
                    if (s3ObjectRange.getRange() != null) {
                        if (!s3ObjectRange.getRange().contains(object.lastModified().toEpochMilli())) {
                            continue;
                        }
                    }
                    found.add(object.key());
                }
            }
            log.info("{} objects has been found for prefix: {}. Few to see: {}", found.size(), prefix, CollectionUtils.getFirstN(found, 5));
            allObjects.addAll(found);
        }

        if (allObjects.isEmpty()) {
            throw DittoRuntimeException.of("No object for prefixes [{}] and time range [{}] has been found", s3ObjectRange.getPrefixes(), s3ObjectRange.getRange());
        }
        if (allObjects.size() < parallelism) {
            log.info("Expected parallelism [{}] can not be fulfilled, because we did not found enough objects [{}]. So new parallelism is {}", parallelism, allObjects.size(), allObjects.size());
            this.parallelism = allObjects.size();
        }

        objectsTotal = allObjects.size();
        List<List<String>> splitted = CollectionUtils.toNParts(allObjects, parallelism);


        List<S3Split> s3Splits = splitted.stream()
                .map(S3Split::new)
                .toList();

        for (int split = 1; split <= parallelism; split++) {
            log.info("Initializing S3 worker {}/{}", split, parallelism);
            S3Worker<T> workerThread = threadFactory.modifyThread(new S3Worker<>(
                    eventProducer,
                    fileRWFactory,
                    workerMap.get(split),
                    counterAggregator,
                    s3Splits.get(split - 1),
                    S3Downloader.fileDownloader(s3Handler, jobId, bucket,
                            new WorkerPathProvider(
                                    jobId,
                                    fileRWFactory.getInfo().parentPath(),
                                    fileRWFactory.getInfo().fileExtension(),
                                    split,
                                    WorkerThread.WorkerType.SOURCE))
            ), split);

            boolean initialized = workerThread.initThread();
            if(!initialized) {
                continue;
            }
            workerThreads.put(workerThread.getWorkerNumber(), workerThread);
        }
        return workerThreads.keySet();
    }

    public long getObjectsTotal() {
        return objectsTotal;
    }

    public static class S3SourceBuilder<T> extends Source.SourceBuilder<T, S3SourceBuilder<T>> {
        private S3ObjectRange s3ObjectRange;
        private String bucket;
        private S3Handler s3Handler;
        private FileRWFactory<T> fileRWFactory;


        public S3SourceBuilder s3ObjectRange(S3ObjectRange s3ObjectRange) {
            this.s3ObjectRange = s3ObjectRange;
            return this;
        }

        public S3SourceBuilder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public S3SourceBuilder s3Handler(S3Handler s3Handler) {
            this.s3Handler = s3Handler;
            return this;
        }

        public S3SourceBuilder fileRWFactory(FileRWFactory<T> fileRWFactory) {
            this.fileRWFactory = fileRWFactory;
            return this;
        }

        @Override
        public S3Source<T> build() {
            return new S3Source<>(parallelism, type, s3ObjectRange, jobId, bucket, s3Handler, eventProducer, fileRWFactory);
        }

        @Override
        protected S3SourceBuilder<T> self() {
            return this;
        }
    }
}
