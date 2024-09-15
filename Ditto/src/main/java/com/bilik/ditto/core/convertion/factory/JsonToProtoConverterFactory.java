package com.bilik.ditto.core.convertion.factory;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.common.ReflectionUtils;
import com.bilik.ditto.core.convertion.Converter;
import com.bilik.ditto.core.type.DataType;

import java.lang.reflect.Method;

public class JsonToProtoConverterFactory implements ConverterFactory<String, Message> {

    @Override
    public Converter<String, Message> createConverter(JobDescriptionInternal jobDescription) {
        return new JsonToProtoConverterFactory.JsonToProtoConverter(
                (Class<? extends Message>) jobDescription.getSinkGeneral().getDataType().getTypeClass());
    }

    @Override
    public DataType inputType() {
        return DataType.JSON;
    }

    @Override
    public DataType outputType() {
        return DataType.PROTOBUF;
    }

    public static class JsonToProtoConverter implements Converter<String, Message> {

        private final Method newBuilderMethod;

        public JsonToProtoConverter(Class<? extends Message> clasz) {
            this.newBuilderMethod = ReflectionUtils.getMethod(clasz, "newBuilder");
        }

        @Override
        public Message convert(String input) throws Exception {
            AbstractMessage.Builder builder = (AbstractMessage.Builder) newBuilderMethod.invoke(null);
            JsonFormat.parser()
                    .ignoringUnknownFields()
                    .merge(input, builder);
            return builder.build();
        }

    }

}
