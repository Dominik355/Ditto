package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.core.io.PathWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfsFileUploaderTest {

    @Test
    void defaultTest(@TempDir(cleanup = CleanupMode.ALWAYS) Path tempDir) throws IOException {
        Path tempFile = Files.createTempFile(tempDir, "test-f", ".tmp");
        PathWrapper fileToUpload = new PathWrapper(tempFile);

        HdfsFileUploader uploader = new HdfsFileUploader(
                new org.apache.hadoop.fs.Path(tempDir.toUri()), FileSystem.getLocal(new Configuration()));

        // when
        boolean uploaded = uploader.upload(fileToUpload);

        // then
        assertThat(uploaded).isTrue();
    }
    
}
