package com.bilik.ditto.core.io;

import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class FileRwInfoTest {

    @Test
    void jsonInfo() {
        var path = Path.of("test/path");
        var info = FileRWInfo.jsonFileInfo(path);
        assertThat(info.fileExtension()).isEqualTo("json");
        assertThat(info.clasz()).isEqualTo(String.class);
        assertThat(info.parentPath()).isEqualTo(path);
    }

    @Test
    void protoInfo() {
        var path = Path.of("test/path");
        var info = FileRWInfo.protoFileInfo(Sensor.class, path);
        assertThat(info.fileExtension()).isEqualTo("parquet");
        assertThat(info.clasz()).isEqualTo(Sensor.class);
        assertThat(info.parentPath()).isEqualTo(path);
    }

    @Test
    void tempInfo() {
        var path = Path.of("test/path");
        var info = FileRWInfo.tempFileInfo(Long.class, path);
        assertThat(info.fileExtension()).isEqualTo("temp");
        assertThat(info.clasz()).isEqualTo(Long.class);
        assertThat(info.parentPath()).isEqualTo(path);
    }

    @Test
    void resolvePathTest() {
        var childFile = "test/dir/testFile";
        var path = Path.of("test/path");
        var info = FileRWInfo.tempFileInfo(Long.class, path);
        var childPath = info.resolveFile(childFile);
        assertThat(childPath.toString()).isEqualTo("test/path/test/dir/testFile.temp");
    }

}
