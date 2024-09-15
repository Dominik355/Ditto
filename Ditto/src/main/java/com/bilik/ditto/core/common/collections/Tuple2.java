package com.bilik.ditto.core.common.collections;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Immutable Tuple2 or Pair implementation ... name it as you wish
 */
public class Tuple2<T1, T2> implements Iterable<Object>, Serializable {

    private static final long serialVersionUID = 1L;

    private final T1 t1;
    private final T2 t2;

    public static <T1, T2> Tuple2<T1, T2> of(T1 t1, T2 t2) {
        return new Tuple2<>(t1, t2);
    }

    public Tuple2(T1 t1, T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public T1 getT1() {
        return t1;
    }

    public T2 getT2() {
        return t2;
    }

    public <R> Tuple2<R, T2> mapT1(Function<T1, R> mapper) {
        return new Tuple2<>(mapper.apply(t1), t2);
    }

    public <R> Tuple2<T1, R> mapT2(Function<T2, R> mapper) {
        return new Tuple2<>(t1, mapper.apply(t2));
    }

    public Object get(int index) {
        switch (index) {
            case 0:
                return t1;
            case 1:
                return t2;
            default:
                return null;
        }
    }

    public List<Object> toList() {
        return Arrays.asList(toArray());
    }

    public Object[] toArray() {
        return new Object[]{t1, t2};
    }

    @Override
    public int hashCode() {
        int result = size();
        result = 21 * result + t1.hashCode();
        result = 21 * result + t2.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Tuple2<?, ?> tuple2) {
            return Objects.equals(this.t1, tuple2.t1)
                    && Objects.equals(this.t2, tuple2.t2);
        }
        return false;
    }

    @Override
    public String toString() {
        return "[T1=" + t1 + ",|T2=" + t2 + "]";
    }

    @Override
    public Iterator<Object> iterator() {
        return Collections.unmodifiableList(toList()).iterator();
    }

    public int size() {
        return 2;
    }
}
