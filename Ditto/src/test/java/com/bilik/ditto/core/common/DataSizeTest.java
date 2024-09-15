package com.bilik.ditto.core.common;

import com.bilik.ditto.core.common.DataSize.DataUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class DataSizeTest {

    @Test
    void ofBytesToBytes() {
        assertThat(DataSize.ofBytes(1024).toBytes()).isEqualTo(1024);
    }

    @Test
    void ofBytesToKilobytes() {
        assertThat(DataSize.ofBytes(1024).toKilobytes()).isEqualTo(1);
    }

    @Test
    void ofKilobytesToKilobytes() {
        assertThat(DataSize.ofKilobytes(1024).toKilobytes()).isEqualTo(1024);
    }

    @Test
    void ofKilobytesToMegabytes() {
        assertThat(DataSize.ofKilobytes(1024).toMegabytes()).isEqualTo(1);
    }

    @Test
    void ofMegabytesToMegabytes() {
        assertThat(DataSize.ofMegabytes(1024).toMegabytes()).isEqualTo(1024);
    }

    @Test
    void ofMegabytesToGigabytes() {
        assertThat(DataSize.ofMegabytes(2048).toGigabytes()).isEqualTo(2);
    }

    @Test
    void ofGigabytesToGigabytes() {
        assertThat(DataSize.ofGigabytes(4096).toGigabytes()).isEqualTo(4096);
    }

    @Test
    void ofGigabytesToTerabytes() {
        assertThat(DataSize.ofGigabytes(4096).toTerabytes()).isEqualTo(4);
    }

    @Test
    void ofTerabytesToGigabytes() {
        assertThat(DataSize.ofTerabytes(1).toGigabytes()).isEqualTo(1024);
    }

    @Test
    void ofWithBytesUnit() {
        assertThat(DataSize.of(10, DataUnit.BYTES)).isEqualTo(DataSize.ofBytes(10));
    }

    @Test
    void ofWithKilobytesUnit() {
        assertThat(DataSize.of(20, DataUnit.KILOBYTES)).isEqualTo(DataSize.ofKilobytes(20));
    }

    @Test
    void ofWithMegabytesUnit() {
        assertThat(DataSize.of(30, DataUnit.MEGABYTES)).isEqualTo(DataSize.ofMegabytes(30));
    }

    @Test
    void ofWithGigabytesUnit() {
        assertThat(DataSize.of(40, DataUnit.GIGABYTES)).isEqualTo(DataSize.ofGigabytes(40));
    }

    @Test
    void ofWithTerabytesUnit() {
        assertThat(DataSize.of(50, DataUnit.TERABYTES)).isEqualTo(DataSize.ofTerabytes(50));
    }

    @Test
    void fromString_Bytes() {
        assertThat(DataSize.fromString("1024 b")).isEqualTo(DataSize.ofKilobytes(1));
    }

    @Test
    void fromStringNegativeNumberWithoutUnit() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> DataSize.fromString("-1"))
                .withMessage("[-1] is not a valid String representing data size");
    }

    @Test
    void fromStringWithoutUnit() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> DataSize.fromString("1024"))
                .withMessage("[1024] is not a valid String representing data size");
    }

    @Test
    void fromStringWithInvalidUnit() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> DataSize.fromString("1024 Wb"))
                .withMessage("[1024 Wb] is not a valid String representing data size");
    }

    @ParameterizedTest(name = "[{index}] text = ''{0}''")
    @ValueSource(strings = {
            "1024B",
            "1024 B",
            "1024B   ",
            "   1024B",
            " 1024B ",
            "\t1024   B\t"
    })
    void fromStringWithBytes(String text) {
        assertThat(DataSize.fromString(text)).isEqualTo(DataSize.ofKilobytes(1));
    }

    @ParameterizedTest(name = "[{index}] text = ''{0}''")
    @ValueSource(strings = {
            "1024kB",
            "1024 kB",
            "1024kB   ",
            "   1024kB",
            " 1024kB ",
            "\t1024   kB\t",
            "1024 kb",
            "1024 Kb",
            "1024 kB",
            "1024 KB",
            "1024 kiB",
            "1024 kib",
            "1024 Kib",
            "1024 KiB",
    })
    void fromStringWithKiloBytes(String text) {
        assertThat(DataSize.fromString(text)).isEqualTo(DataSize.ofMegabytes(1));
    }

    @ParameterizedTest(name = "[{index}] text = ''{0}''")
    @ValueSource(strings = {
            "1024mB",
            "1024 mB",
            "1024mB   ",
            "   1024mB",
            " 1024mB ",
            "\t1024   mB\t",
            "1024 mb",
            "1024 Mb",
            "1024 mB",
            "1024 MB",
            "1024 miB",
            "1024 mib",
            "1024 Mib",
            "1024 MiB",
    })
    void fromStringWithMegaBytes(String text) {
        assertThat(DataSize.fromString(text)).isEqualTo(DataSize.ofGigabytes(1));
    }

    @ParameterizedTest(name = "[{index}] text = ''{0}''")
    @ValueSource(strings = {
            "1024gB",
            "1024 gB",
            "1024gB   ",
            "   1024gB",
            " 1024gB ",
            "\t1024   gB\t",
            "1024 gb",
            "1024 Gb",
            "1024 gB",
            "1024 GB",
            "1024 giB",
            "1024 gib",
            "1024 Gib",
            "1024 GiB",
    })
    void fromStringWithGigaBytes(String text) {
        assertThat(DataSize.fromString(text)).isEqualTo(DataSize.ofTerabytes(1));
    }

    @ParameterizedTest(name = "[{index}] text = ''{0}''")
    @ValueSource(strings = {
            "1024tB",
            "1024 tB",
            "1024tB   ",
            "   1024tB",
            " 1024tB ",
            "\t1024   tB\t",
            "1024 tb",
            "1024 Tb",
            "1024 tB",
            "1024 TB",
            "1024 tiB",
            "1024 tib",
            "1024 Tib",
            "1024 TiB",
    })
    void fromStringWithTeraBytes(String text) {
        assertThat(DataSize.fromString(text)).isEqualTo(DataSize.ofTerabytes(1024));
    }

    @Test
    void toStringUsesBytes() {
        assertThat(DataSize.ofKilobytes(1).toString()).isEqualTo("1024B");
    }

    @Test
    void toStringUsesMegaBytes() {
        assertThat(DataSize.ofKilobytes(1).toString(DataUnit.KILOBYTES)).isEqualTo("1KB");
    }
    
    @Test
    void convertion() {
        assertThat(DataSize.ofGigabytes(1).toKilobytes())
                .isEqualTo(DataSize.ofMegabytes(1024).toKilobytes());
    }

}