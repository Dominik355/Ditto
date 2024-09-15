package com.bilik.ditto.core.convertion.factory;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.convertion.Converter;
import com.bilik.ditto.core.type.DataType;

public class ProtoToJsonConverterFactory implements ConverterFactory<Message, String> {

    @Override
    public Converter<Message, String> createConverter(JobDescriptionInternal jobDescription) {
        return new ProtoToJsonConverter();
    }

    @Override
    public DataType inputType() {
        return DataType.PROTOBUF;
    }

    @Override
    public DataType outputType() {
        return DataType.JSON;
    }


    public static class ProtoToJsonConverter implements Converter<Message, String> {

        private final JsonFormat.Printer printer;

        public ProtoToJsonConverter() {
            this.printer = JsonFormat.printer();
        }

        @Override
        public String convert(Message input) throws Exception {
            return printer.print(input);
        }

    }
}
