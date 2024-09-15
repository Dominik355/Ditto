package com.bilik.ditto.core.type;

import java.io.IOException;

public abstract class TypeSerde<T> {

    public abstract Serializer<T> serializer();

    public abstract Deserializer<T> deserializer();

    public interface Serializer<T> {
        byte[] serialize(T record);
    }

    public interface Deserializer<T> {
        T deserialize(byte[] data) throws IOException;
    }
}
