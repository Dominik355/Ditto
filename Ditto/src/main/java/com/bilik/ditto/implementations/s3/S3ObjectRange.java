package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import org.apache.commons.lang3.Range;

import java.util.List;
import java.util.StringJoiner;

/**
 * Holder of values that define which objects from S3 bucket should be processed by the job
 */
public class S3ObjectRange {

    private final Range<Long> range;
    private final List<String> prefixes;

    public S3ObjectRange(Range<Long> range, List<String> prefixes) {
        this.range = range;
        this.prefixes = prefixes;
    }

    public static S3ObjectRange of(Range<Long> range, List<String> prefixes) {
        return new S3ObjectRange(range, prefixes);
    }

    public static S3ObjectRange of(long from, long to, List<String> prefixes) {
        return new S3ObjectRange(Range.between(from, to), prefixes);
    }

    public static S3ObjectRange from(JobDescriptionInternal.S3Source jobDescriptionSource) {
        return new S3ObjectRange(
                Range.between(
                        jobDescriptionSource.getFrom() != null ? jobDescriptionSource.getFrom() : 0L,
                        jobDescriptionSource.getTo()  != null ? jobDescriptionSource.getTo() : Long.MAX_VALUE),
                jobDescriptionSource.getObjectNamePrefixes());
    }

    public long getFrom() {
        return this.range.getMinimum();
    }

    public long getTo() {
        return this.range.getMaximum();
    }

    public Range<Long> getRange() {
        return this.range;
    }

    public List<String> getPrefixes() {
        return prefixes;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", S3ObjectRange.class.getSimpleName() + "[", "]")
                .add("range=" + range)
                .add("prefixes=" + prefixes)
                .toString();
    }
}
