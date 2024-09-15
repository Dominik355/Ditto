package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.util.WorkerPathProvider;
import com.bilik.ditto.core.util.Preconditions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * S3Downloader is already bucket-specific
 */
public abstract class S3Downloader<OUT> implements AutoCloseable {

    protected final S3Handler s3Handler;
    protected final String jobId;
    protected final String bucket;

    protected S3Downloader(S3Handler s3Handler, String jobId, String bucket) {
        this.s3Handler = Objects.requireNonNull(s3Handler);
        this.jobId = Preconditions.requireNotBlank(jobId);
        this.bucket = bucket;
    }

    public abstract OUT download(String objectKey);

    @Override
    public void close() throws Exception {
        this.s3Handler.close();
    }

    public static ByteS3Downloader byteDownloader(S3Handler s3Handler, String jobId, String bucket) {
        return new ByteS3Downloader(s3Handler, jobId, bucket);
    }

    public static FileS3Downloader fileDownloader(S3Handler s3Handler, String jobId, String bucket, WorkerPathProvider workerPathProvider) {
        return new FileS3Downloader(s3Handler, jobId, bucket, workerPathProvider);
    }

    // IMPLEMENTATIONS

    public static class FileS3Downloader extends S3Downloader<Path> {

        private final WorkerPathProvider workerPathProvider;

        public FileS3Downloader(S3Handler s3Handler,
                                String jobId,
                                String bucket,
                                WorkerPathProvider workerPathProvider) {
            super(s3Handler, jobId, bucket);
            this.workerPathProvider = Objects.requireNonNull(workerPathProvider);

            try {
                Files.createDirectories(workerPathProvider.getParent().getNioPath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Path download(String objectKey) {
            Path path = workerPathProvider.get().getNioPath();
            s3Handler.downloadToFile(bucket, objectKey, path);
            return path;
        }

    }

    public static class ByteS3Downloader extends S3Downloader<byte[]> {

        protected ByteS3Downloader(S3Handler s3Handler, String jobId, String bucket) {
            super(s3Handler, jobId, bucket);
        }

        @Override
        public byte[] download(String objectKey) {
            return s3Handler
                    .downloadToBytes(bucket, objectKey)
                    .asByteArrayUnsafe();
        }
    }

}
