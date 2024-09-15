package com.bilik.ditto.core.io;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class PathWrapperTest {

    @Test
    void nullValue() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new PathWrapper(null));
    }

    @Test
    void notNull_wontThrowException() {
        PathWrapper pathWrapper = new PathWrapper(Path.of("/test/path/to/dir"));

        assertThat(pathWrapper.getNioPath()).isNotNull();
        assertThat(pathWrapper.getHadoopPath()).isNotNull();
    }

    @Test
    void comparison() {
        PathWrapper pathWrapper1 = new PathWrapper(Path.of("/test/path/to/dir"));
        PathWrapper pathWrapper2 = new PathWrapper(Path.of("/test/path/to/dir"));

        assertThat(pathWrapper1).isEqualTo(pathWrapper2);
    }


}
