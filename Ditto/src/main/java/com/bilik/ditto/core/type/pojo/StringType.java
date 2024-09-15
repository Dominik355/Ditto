package com.bilik.ditto.core.type.pojo;

import com.bilik.ditto.core.type.DataType;
import com.bilik.ditto.core.type.TypeSerde;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringType extends BasicType<String> {

    @Override
    public DataType dataType() {
        return DataType.STRING;
    }

    @Override
    public Class<String> getTypeClass() {
        return String.class;
    }

    @Override
    public TypeSerde<String> createSerde() {
        return new StringType.StringSerde();
    }

    @Override
    public String name() {
        return String.class.getSimpleName();
    }

    public static class StringSerde extends TypeSerde<String> {

        private Charset encoding = StandardCharsets.UTF_8;

        @Override
        public Serializer<String> serializer() {
            return input -> (input == null) ?
                    null : input.getBytes(encoding);
        }

        @Override
        public Deserializer<String> deserializer() {
            return byteArr -> (byteArr == null) ?
                    null : new String(byteArr, encoding);
        }
    }

}
