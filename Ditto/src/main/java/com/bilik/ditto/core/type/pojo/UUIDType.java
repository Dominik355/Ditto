package com.bilik.ditto.core.type.pojo;

import com.bilik.ditto.core.exception.SerializationException;
import com.bilik.ditto.core.type.DataType;
import com.bilik.ditto.core.type.TypeSerde;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDType extends BasicType<UUID> {

    @Override
    public DataType dataType() {
        return DataType.UUID;
    }

    @Override
    public Class<UUID> getTypeClass() {
        return UUID.class;
    }

    @Override
    public TypeSerde<UUID> createSerde() {
        return new UUIDType.UUIDSerde();
    }

    @Override
    public String name() {
        return UUID.class.getSimpleName();
    }

    public static class UUIDSerde extends TypeSerde<UUID> {

        private static final int BYTES = 16;

        @Override
        public Serializer<UUID> serializer() {
            return input -> {
                ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
                bb.putLong(input.getLeastSignificantBits());
                bb.putLong(input.getMostSignificantBits());
                return bb.array();
            };
        }

        @Override
        public Deserializer<UUID> deserializer() {
            return data -> {
                if (data.length != BYTES) {
                    throw new SerializationException("UUID data should be " + BYTES + "bytes, but it is " + data.length);
                }
                ByteBuffer bb = ByteBuffer.wrap(data);
                return new UUID(bb.getLong(), bb.getLong());
            };
        }
    }

}
