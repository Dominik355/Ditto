package com.bilik.ditto.implementations.hdfs;

import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.Range;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Holder of values that define which files from HDFS should be processed by the job
 */
public class HdfsFileRange {

    private final String parentPath;
    private final boolean recursive;
    private final Set<String> prefixes;
    private final Range<Long> range;


    public HdfsFileRange(String parentPath, boolean recursive, Set<String> prefixes, Range<Long> range) {
        this.parentPath = parentPath;
        this.recursive = recursive;
        this.prefixes = prefixes;
        this.range = range;
    }

    public static HdfsFileRange from(JobDescriptionInternal.HdfsSource jobDescriptionSource) {
        return new HdfsFileRange(
                jobDescriptionSource.getParentPath(),
                jobDescriptionSource.isRecursive(),
                jobDescriptionSource.getPrefixes() != null ? new HashSet<>(jobDescriptionSource.getPrefixes()) : Collections.emptySet(),
                Range.between(
                        jobDescriptionSource.getFrom() != null ? jobDescriptionSource.getFrom() : 0L,
                        jobDescriptionSource.getTo()  != null ? jobDescriptionSource.getTo() : Long.MAX_VALUE)
        );
    }


    public String getStringParentPath() {
        return parentPath;
    }

    public Path getParentPath() {
        return new Path(parentPath);
    }

    public boolean isRecursive() {
        return recursive;
    }

    public Set<String> getPrefixes() {
        return prefixes;
    }

    public Range<Long> getRange() {
        return range;
    }

    public long getFrom() {
        return this.range.getMinimum();
    }

    public long getTo() {
        return this.range.getMaximum();
    }

    public boolean fulfillsPredicates(LocatedFileStatus fileStatus) {
        return hasPrefix(fileStatus) && isWithinRange(fileStatus);
    }

    /**
     * Check if final component of fileStatus's path starts with defined prefix
     */
    public boolean hasPrefix(LocatedFileStatus fileStatus) {
        if (CollectionUtils.isNotEmpty(prefixes)) {
            for (String prefix : prefixes) {
                if (fileStatus.getPath().getName().startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    public boolean isWithinRange(LocatedFileStatus fileStatus) {
        if (range != null) {
            return range.contains(fileStatus.getModificationTime());
        }
        return true;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", HdfsFileRange.class.getSimpleName() + "[", "]")
                .add("parentPath=" + parentPath)
                .add("recursive=" + recursive)
                .add("prefixes='" + prefixes + "'")
                .add("range=" + range)
                .toString();
    }
}
