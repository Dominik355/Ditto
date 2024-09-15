package com.bilik.ditto.core.type.notPojo.protobuf;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.bilik.ditto.api.service.providers.ProtobufTypeResolver;
import com.bilik.ditto.core.type.DataType;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.type.TypeSerde;
import com.bilik.ditto.core.util.ProtoUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ProtobufType<T extends Message> extends Type<T> {

    private final Class<T> clasz;

    public ProtobufType(ProtobufTypeResolver.ProtobufRegisterInfo info) {
        this((Class<T>) info.clasz());
    }

    public ProtobufType(Class<T> clasz) {
        this.clasz = clasz;
    }

    @Override
    public DataType dataType() {
        return DataType.PROTOBUF;
    }

    @Override
    public Class<T> getTypeClass() {
        return clasz;
    }

    @Override
    public TypeSerde<T> createSerde() {
        return new ProtobufSerde<>(clasz);
    }

    @Override
    public String name() {
        return "Protobuf-" + clasz.getName();
    }

    public static class ProtobufSerde<T extends Message> extends TypeSerde<T> {

        private final Class<T> clasz;

        public ProtobufSerde(Class<T> clasz) {
            this.clasz = clasz;
        }

        @Override
        public Serializer<T> serializer() {
            return MessageLite::toByteArray;
        }

        @Override
        public Deserializer<T> deserializer() {
            Method parseFrom = ProtoUtils.getParseFromMethod(clasz);
            return data -> {
                try {
                    return (T) parseFrom.invoke(null, data);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}
