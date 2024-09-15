package com.bilik.ditto.core.common;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataSize implements Serializable, Comparable<DataSize> {

    private static final long BYTES_PER_KB = 1024;
    private static final long BYTES_PER_MB = BYTES_PER_KB * 1024;
    private static final long BYTES_PER_GB = BYTES_PER_MB * 1024;
    private static final long BYTES_PER_TB = BYTES_PER_GB * 1024;

    private static final Map<Pattern, Function<Long, DataSize>> BYTE_RESOLVER_MAP;
    static {
        BYTE_RESOLVER_MAP = Map.of(
                Pattern.compile("(\\d+)\\s*?(B|b)"), DataSize::ofBytes,
                Pattern.compile("(\\d+)\\s*?(K|k)((B|b)|(iB|ib))"), DataSize::ofKilobytes,
                Pattern.compile("(\\d+)\\s*?(M|m)((B|b)|(iB|ib))"), DataSize::ofMegabytes,
                Pattern.compile("(\\d+)\\s*?(G|g)((B|b)|(iB|ib))"), DataSize::ofGigabytes,
                Pattern.compile("(\\d+)\\s*?(T|t)((B|b)|(iB|ib))"), DataSize::ofTerabytes
        );
    }

    /**
     * Actual value in pure bytes
     */
    private final long bytes;

    private DataSize(long bytes) {
        this.bytes = bytes;
    }

    /**
     * @return DataSize represented by supplied String
     * @throws IllegalArgumentException if provided String is not valid representation of DataSize
     */
    public static DataSize fromString(String dataSize) {
        dataSize = dataSize.trim();
        for (Map.Entry<Pattern, Function<Long, DataSize>> entry : BYTE_RESOLVER_MAP.entrySet()) {
            Matcher matcher = entry.getKey().matcher(dataSize);
            if (matcher.matches()) {
                return entry.getValue().apply(Long.valueOf(matcher.group(1)));
            }
        }
        throw new IllegalArgumentException("[" + dataSize + "] is not a valid String representing data size");
    }

    public static DataSize ofBytes(long bytes) {
        return new DataSize(bytes);
    }

    public static DataSize ofKilobytes(long kilobytes) {
        return new DataSize(Math.multiplyExact(kilobytes, BYTES_PER_KB));
    }

    public static DataSize ofMegabytes(long megabytes) {
        return new DataSize(Math.multiplyExact(megabytes, BYTES_PER_MB));
    }

    public static DataSize ofGigabytes(long gigabytes) {
        return new DataSize(Math.multiplyExact(gigabytes, BYTES_PER_GB));
    }

    public static DataSize ofTerabytes(long terabytes) {
        return new DataSize(Math.multiplyExact(terabytes, BYTES_PER_TB));
    }

    public static DataSize of(long amount, DataUnit unit) {
        Objects.requireNonNull(unit, "Unit must not be null");
        return new DataSize(Math.multiplyExact(amount, unit.size().toBytes()));
    }

    public long toBytes() {
        return this.bytes;
    }

    public long toKilobytes() {
        return Math.floorDiv(this.bytes, BYTES_PER_KB);
    }

    public long toMegabytes() {
        return Math.floorDiv(this.bytes, BYTES_PER_MB);
    }

    public long toGigabytes() {
        return Math.floorDiv(this.bytes, BYTES_PER_GB);
    }

    public long toTerabytes() {
        return Math.floorDiv(this.bytes, BYTES_PER_TB);
    }

    @Override
    public int compareTo(DataSize other) {
        return Long.compare(this.bytes, other.bytes);
    }

    @Override
    public String toString() {
        return String.format("%dB", this.bytes);
    }

    public String toString(DataUnit unit) {
        return String.format("%d%s", Math.floorDiv(this.bytes, unit.size().toBytes()), unit.suffix);
    }


    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        DataSize otherSize = (DataSize) other;
        return (this.bytes == otherSize.bytes);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.bytes);
    }

    public enum DataUnit {

        BYTES("B", DataSize.ofBytes(1)),
        KILOBYTES("KB", DataSize.ofKilobytes(1)),
        MEGABYTES("MB", DataSize.ofMegabytes(1)),
        GIGABYTES("GB", DataSize.ofGigabytes(1)),
        TERABYTES("TB", DataSize.ofTerabytes(1));


        private final String suffix;
        private final DataSize size;


        DataUnit(String suffix, DataSize size) {
            this.suffix = suffix;
            this.size = size;
        }

        DataSize size() {
            return this.size;
        }

        public static DataUnit fromSuffix(String suffix) {
            for (DataUnit candidate : values()) {
                if (candidate.suffix.equals(suffix)) {
                    return candidate;
                }
            }
            throw new IllegalArgumentException("Unknown data unit suffix '" + suffix + "'");
        }
    }

}
