package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.job.output.accumulation.FileUploader;

import java.io.IOException;
import java.nio.file.Path;

public abstract class S3Uploader<OUT> implements AutoCloseable {

    protected final S3Handler s3Handler;
    protected final String jobId;
    protected final String bucket;

    protected S3Uploader(S3Handler s3Handler, String jobId, String bucket) {
        this.s3Handler = s3Handler;
        this.jobId = jobId;
        this.bucket = bucket;
    }

    /**
     * @return true if upload was successful
     */
    public abstract boolean upload(String objectKey, OUT data);

    @Override
    public void close() throws Exception {
        this.s3Handler.close();
    }

    // Factory methods
    public static ByteS3Uploader byteUploader(S3Handler s3Handler, String jobId, String bucket) {
        return new ByteS3Uploader(s3Handler, jobId, bucket);
    }

    public static FileS3Uploader fileUploader(S3Handler s3Handler, String jobId, String bucket) {
        return new FileS3Uploader(s3Handler, jobId, bucket);
    }

    // IMPLEMENTATIONS

    public static class FileS3Uploader extends S3Uploader<Path> implements FileUploader {

        public FileS3Uploader(S3Handler s3Handler, String jobId, String bucket) {
            super(s3Handler, jobId, bucket);
        }

        @Override
        public boolean upload(String objectKey, Path data) {
            return s3Handler.upload(bucket, objectKey, data);
        }

        @Override
        public boolean upload(PathWrapper path) {
            String name = path.getNioPath().getFileName().toString();
            return this.upload(name.substring(0, name.lastIndexOf('.')), path.getNioPath());
        }

        @Override
        public void close() throws IOException {
            this.s3Handler.close();
        }
    }

    public static class ByteS3Uploader extends S3Uploader<byte[]> {

        public ByteS3Uploader(S3Handler s3Handler, String jobId, String bucket) {
            super(s3Handler, jobId, bucket);
        }

        @Override
        public boolean upload(String objectKey, byte[] data) {
            return s3Handler.upload(bucket, objectKey, data);
        }
    }

}
