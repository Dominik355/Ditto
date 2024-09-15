package com.bilik.ditto.core.type.pojo;

import com.google.common.primitives.Ints;
import com.bilik.ditto.core.exception.SerializationException;
import com.bilik.ditto.core.type.DataType;
import com.bilik.ditto.core.type.TypeSerde;

public class IntegerType extends BasicType<Integer> {

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    public Class<Integer> getTypeClass() {
        return Integer.class;
    }

    @Override
    public TypeSerde<Integer> createSerde() {
        return new IntegerType.IntegerSerde();
    }

    @Override
    public String name() {
        return Integer.class.getSimpleName();
    }

    public static class IntegerSerde extends TypeSerde<Integer> {

        private static final int BYTES = 4;

        @Override
        public Serializer<Integer> serializer() {
            return data -> {
                if (data == null)
                    return null;
                return Ints.toByteArray(data);
            };
        }

        @Override
        public Deserializer<Integer> deserializer() {
            return data -> {
                if (data == null)
                    return null;
                if (data.length != BYTES)
                    throw new SerializationException("Size of data received by Integer Deserializer is not " + BYTES);
                return Ints.fromByteArray(data);
            };
        }
    }

}