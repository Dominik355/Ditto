package com.bilik.ditto.core.job;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * Why is 'isLast' separated and not every null value is simply considered last element ?
 * Because we don't want ot support null values, those should not happen.
 * This way it is safe to supply last element, when it needs to be explicitly defined.
 */
public class StreamElement<T> {

    private static final long DEFAULT_TIMESTAMP = -1L;

    private final T value;
    private final boolean isLast;
    private final long timestamp;

    public StreamElement(T value) {
        this (value, false);
    }


    public StreamElement(T value, boolean isLast) {
        this(value, isLast, DEFAULT_TIMESTAMP);
    }

    public StreamElement(T value, boolean isLast, long timestamp) {
        if (value == null && !isLast) {
            throw new IllegalArgumentException("Value of StreamElement can not be null if it's not last element !!!");
        }
        this.value = value;
        this.isLast = isLast;
        this.timestamp = timestamp;
    }

    public static <T> StreamElement<T> of(T value) {
        return new StreamElement<>(value);
    }

    public static <T> StreamElement<T> of(T value, long timestamp) {
        return new StreamElement<>(value, false, timestamp);
    }

    public static <T> StreamElement<T> of(T value, StreamElement origin) {
        return new StreamElement<>(value, origin.isLast, origin.timestamp);
    }

    public static <T> StreamElement<T> last() {
        return new StreamElement<>(null, true);
    }

    public T value() {
        return value;
    }

    public boolean isLast() {
        return isLast;
    }

    public boolean hasTimestamp() {
        return timestamp > 0;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamElement<?> that = (StreamElement<?>) o;
        return isLast == that.isLast && timestamp == that.timestamp && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, isLast, timestamp);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StreamElement.class.getSimpleName() + "[", "]")
                .add("value=" + value)
                .add("isLast=" + isLast)
                .add("timestamp=" + timestamp)
                .toString();
    }
}
