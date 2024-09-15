package com.bilik.ditto.core.type.pojo;

import com.google.common.primitives.Longs;
import com.bilik.ditto.core.exception.SerializationException;
import com.bilik.ditto.core.type.DataType;
import com.bilik.ditto.core.type.TypeSerde;

public class LongType extends BasicType<Long> {

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    public Class<Long> getTypeClass() {
        return Long.class;
    }

    @Override
    public TypeSerde<Long> createSerde() {
        return new LongSerde();
    }

    @Override
    public String name() {
        return Long.class.getSimpleName();
    }

    public static class LongSerde extends TypeSerde<Long> {

        private static final int BYTES = 8;

        @Override
        public Serializer<Long> serializer() {
            return data -> {
                if (data == null)
                    return null;
                return Longs.toByteArray(data);
            };
        }

        @Override
        public Deserializer<Long> deserializer() {
            return data -> {
                if (data == null)
                    return null;
                if (data.length != BYTES)
                    throw new SerializationException("Size of data received by Long Deserializer is not " + BYTES);
                return Longs.fromByteArray(data);
            };
        }
    }

}
