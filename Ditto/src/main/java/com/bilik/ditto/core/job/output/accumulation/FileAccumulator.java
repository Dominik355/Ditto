package com.bilik.ditto.core.job.output.accumulation;

import com.bilik.ditto.core.common.DataSize;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.FileWriter;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.util.FileUtils;
import com.bilik.ditto.core.job.StreamElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

public class FileAccumulator<IN> implements SinkAccumulator<IN> {

    private static final Logger log = LoggerFactory.getLogger(FileAccumulator.class);

    private final FileRWFactory<IN> writerFactory;
    private final Predicate<FileWriter> isFullCondition;
    private final FileUploader uploader;

    private FileWriter<IN> writer; // created with writerFactory

    private final String jobId;
    private final int workerId;
    private int count;

    private PathWrapper currentPath;

    public FileAccumulator(FileRWFactory<IN> writerFactory,
                           Predicate<FileWriter> isFullCondition,
                           FileUploader uploader,
                           String jobId,
                           int workerId) {
        this.writerFactory = Objects.requireNonNull(writerFactory);
        this.isFullCondition = Objects.requireNonNull(isFullCondition);
        this.uploader = Objects.requireNonNull(uploader);
        this.jobId = Objects.requireNonNull(jobId);
        this.workerId = workerId;
        // create first writer
        try {
            this.currentPath = newPath();
            this.writer = writerFactory.buildFileWriter(currentPath);
        } catch (Exception e) {
            throw new DittoRuntimeException(e);
        }
    }

    @Override
    public void collect(StreamElement<IN> element) throws InterruptedException, IOException {
        writer.write(element.value());
    }

    @Override
    public boolean isFull() {
        return isFullCondition.test(writer);
    }

    /**
     * Create runnable for current file and assign new writer.
     * After last element, this creates empty file, which won't be used, neither deleted, because job wil be finished.
     */
    @Override
    public Runnable onFull() throws Exception {

        // store current values
        PathWrapper toSend = currentPath;
        DataSize currentDataSize = DataSize.ofBytes(writer.size());
        long written = writer.writtenElements();

        // close
        writer.close();

        // reset
        currentPath = newPath();
        writer = writerFactory.buildFileWriter(currentPath);

        // return runnable for sinking data
        return () -> {
            log.info("Sending file {} with {} elements of total size {}", toSend, written, currentDataSize);
            boolean success = uploader.upload(toSend);

            // delete file before throwing exception if upload wasn't a success
            try {
                FileUtils.deleteFile(toSend.getNioPath());
            } catch (IOException ex) {
                throw DittoRuntimeException.of(ex,"Failed to delete file {}", toSend);
            }

            if (!success) {
                throw DittoRuntimeException.of("Error occured when uploading File {}", toSend);
            }
        };

    }

    @Override
    public Runnable onFinish() throws Exception {
        return onFull();
    }

    @Override
    public int size() {
        return (int) writer.writtenElements();
    }

    /**
     * 'sink' part is crucial, because if both source and sink needs to store files, overwriting will happen
     * Filenames: "{jobId}/sink/{workerId}/{jobId}-{workerId}-{count}.{suffix}"
     */
    private PathWrapper newPath() {
        String newFile = String.format("%s/%s/%d/%s-%d-%d",
                jobId,
                "sink",
                workerId,
                jobId,
                workerId,
                count++);
        return new PathWrapper(writerFactory.getInfo().resolveFile(newFile));
    }

    @Override
    public void close() throws IOException {
        uploader.close();
        writer.close();
    }

    public static class DataSizeCondition implements Predicate<FileWriter> {

        private final DataSize maxDataSize;

        public DataSizeCondition(DataSize maxDataSize) {
            this.maxDataSize = maxDataSize;
        }

        /**
         * Test if file is full. Return false only if data is lower than max allowed value.
         * Predicate's method name doesn't make sense here, but it is wrapped in FileAccumulator's isFull() method
         */
        @Override
        public boolean test(FileWriter fileWriter) {
            try {
                return DataSize.ofBytes(fileWriter.size()).compareTo(maxDataSize) >= 0;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public static class ElementCountCondition implements Predicate<FileWriter> {

        private final long maxElements;

        public ElementCountCondition(long maxElements) {
            this.maxElements = maxElements;
        }

        /**
         * Test if file is full. Return false only element count is lower than max allowed elements.
         * Predicate's method name doesn't make sense here, but it is wrapped in FileAccumulator's isFull() method
         */
        @Override
        public boolean test(FileWriter fileWriter) {
            return fileWriter.writtenElements() >= maxElements;
        }

    }
}
