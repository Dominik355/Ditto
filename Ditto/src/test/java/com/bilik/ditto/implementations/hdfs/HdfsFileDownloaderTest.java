package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.concurrent.WorkerThread.WorkerType;
import com.bilik.ditto.core.util.WorkerPathProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfsFileDownloaderTest {

    private static final String JOB_ID = "job-id";

    @Test
    void defaultTest(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws IOException {
        Path tempFile = Files.createTempFile(tempDir, "test-f", ".tmp");
        WorkerPathProvider pathProvider = new WorkerPathProvider(
                JOB_ID, tempDir, "tmp", 1, WorkerType.SOURCE);
        HdfsFileDownloader downloader = new HdfsFileDownloader(pathProvider, FileSystem.getLocal(new Configuration()));

        // when
        Path downloaded = downloader.download(new org.apache.hadoop.fs.Path(tempFile.toUri()));

        // then
        WorkerPathProvider assertProvider = new WorkerPathProvider(
                JOB_ID, tempDir, "tmp", 1, WorkerType.SOURCE);
        assertThat(downloaded).isEqualTo(assertProvider.get().getNioPath());
    }

}
