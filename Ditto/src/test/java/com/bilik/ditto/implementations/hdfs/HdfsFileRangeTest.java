package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
    import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfsFileRangeTest {

    private final String PARENT_PATH = "/parent/path";
    private final long from = 100;
    private final long to = 1_000;

    @Test
    void test_withinRange_noPrefix() {
        JobDescriptionInternal.HdfsSource hdfsSource = createSource(true);
        HdfsFileRange fileRange = HdfsFileRange.from(hdfsSource);
        LocatedFileStatus fileStatus = createStatus("daco.txt", from + 1);

        assertThat(fileRange.fulfillsPredicates(fileStatus)).isTrue();
        assertThat(fileRange.isWithinRange(fileStatus)).isTrue();
        assertThat(fileRange.hasPrefix(fileStatus)).isTrue();
    }

    @Test
    void test_beforeRange_noPrefix() {
        JobDescriptionInternal.HdfsSource hdfsSource = createSource(true);
        HdfsFileRange fileRange = HdfsFileRange.from(hdfsSource);
        LocatedFileStatus fileStatus = createStatus("daco.txt", from - 1);

        assertThat(fileRange.fulfillsPredicates(fileStatus)).isFalse();
        assertThat(fileRange.isWithinRange(fileStatus)).isFalse();
        assertThat(fileRange.hasPrefix(fileStatus)).isTrue();
    }

    @Test
    void test_afterRange_noPrefix() {
        JobDescriptionInternal.HdfsSource hdfsSource = createSource(true);
        HdfsFileRange fileRange = HdfsFileRange.from(hdfsSource);
        LocatedFileStatus fileStatus = createStatus("daco.txt", to + 1);

        assertThat(fileRange.fulfillsPredicates(fileStatus)).isFalse();
        assertThat(fileRange.isWithinRange(fileStatus)).isFalse();
        assertThat(fileRange.hasPrefix(fileStatus)).isTrue();
    }

    @Test
    void test_edgeOfRange1_noPrefix() {
        JobDescriptionInternal.HdfsSource hdfsSource = createSource(true);
        HdfsFileRange fileRange = HdfsFileRange.from(hdfsSource);
        LocatedFileStatus fileStatus = createStatus("daco.txt", to);

        assertThat(fileRange.fulfillsPredicates(fileStatus)).isTrue();
        assertThat(fileRange.isWithinRange(fileStatus)).isTrue();
        assertThat(fileRange.hasPrefix(fileStatus)).isTrue();
    }

    @Test
    void test_edgeOfRange2_noPrefix() {
        JobDescriptionInternal.HdfsSource hdfsSource = createSource(true);
        HdfsFileRange fileRange = HdfsFileRange.from(hdfsSource);
        LocatedFileStatus fileStatus = createStatus("daco.txt", from);

        assertThat(fileRange.fulfillsPredicates(fileStatus)).isTrue();
        assertThat(fileRange.isWithinRange(fileStatus)).isTrue();
        assertThat(fileRange.hasPrefix(fileStatus)).isTrue();
    }

    @Test
    void test_hasPrefix_1_undefinedRange() {
        JobDescriptionInternal.HdfsSource hdfsSource = createSource(false, "d", "i");
        HdfsFileRange fileRange = HdfsFileRange.from(hdfsSource);
        LocatedFileStatus fileStatus = createStatus("daco.txt", from);

        assertThat(fileRange.fulfillsPredicates(fileStatus)).isTrue();
        assertThat(fileRange.isWithinRange(fileStatus)).isTrue();
        assertThat(fileRange.hasPrefix(fileStatus)).isTrue();
    }

    @Test
    void test_hasPrefix_2_undefinedRange() {
        JobDescriptionInternal.HdfsSource hdfsSource = createSource(false, "d", "i");
        HdfsFileRange fileRange = HdfsFileRange.from(hdfsSource);
        LocatedFileStatus fileStatus = createStatus("ikl.txt", from);

        assertThat(fileRange.fulfillsPredicates(fileStatus)).isTrue();
        assertThat(fileRange.isWithinRange(fileStatus)).isTrue();
        assertThat(fileRange.hasPrefix(fileStatus)).isTrue();
    }

    @Test
    void test_hasNotPrefix_undefinedRange() {
        JobDescriptionInternal.HdfsSource hdfsSource = createSource(false, "d", "i");
        HdfsFileRange fileRange = HdfsFileRange.from(hdfsSource);
        LocatedFileStatus fileStatus = createStatus("test.txt", from);

        assertThat(fileRange.fulfillsPredicates(fileStatus)).isFalse();
        assertThat(fileRange.isWithinRange(fileStatus)).isTrue();
        assertThat(fileRange.hasPrefix(fileStatus)).isFalse();
    }

    private JobDescriptionInternal.HdfsSource createSource(boolean setRange, String... prefixes) {
        Long lFrom = null, lTo = null;
        if (setRange) {
            lFrom = from;
            lTo = to;
        }
        return new JobDescriptionInternal.HdfsSource(
                "cluster",
                PARENT_PATH,
                false,
                Arrays.asList(prefixes),
                lFrom,
                lTo
        );
    }

    private LocatedFileStatus createStatus(String path, long modificationTime) {
        return new LocatedFileStatus(
                new FileStatus(
                        10,
                        false,
                        0,
                        10,
                        modificationTime,
                        new Path(path)
                ),
                null);
    }

}
