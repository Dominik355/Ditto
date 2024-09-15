package com.bilik.ditto.core.convertion.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.bilik.ditto.transformation.NestedDataToFlatMapTransformation;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.api.service.providers.JsonFlatMapTransformerProvider;
import com.bilik.ditto.core.convertion.Converter;
import com.bilik.ditto.core.convertion.SpecialConverter;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.type.DataType;
import com.bilik.ditto.core.util.ProtoUtils;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class ProtoToFlattedJsonConverterFactory implements SpecialConverterFactory<Message, String> {

    public static final String NAME = "proto-flat-json";

    @Override
    public Converter<Message, String> createConverter(JobDescriptionInternal jobDescription) {
        Objects.requireNonNull(jobDescription.getConverter().getSpecificType(), "Invalid specific type");
        return new ProtoToFlattedJsonConverter(
                JsonFlatMapTransformerProvider.getFlatMapTransformationJson(
                                jobDescription.getConverter().getSpecificType())
                        .orElseThrow(() -> DittoRuntimeException.of("ProtoToFlattedJsonConverter for schema {} was not found", jobDescription.getConverter().getSpecificType())),
                ProtoUtils.createBuilder((Class<? extends Message>) jobDescription.getSourceGeneral().getDataType().getTypeClass()));
    }

    @Override
    public DataType inputType() {
        return DataType.PROTOBUF;
    }

    @Override
    public DataType outputType() {
        return DataType.JSON;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Set<String> specificTypes() {
        return JsonFlatMapTransformerProvider.getAvailableFlatMapTransformations();
    }

    @Override
    public Set<String> args() {
        return Collections.EMPTY_SET;
    }

    public static class ProtoToFlattedJsonConverter implements SpecialConverter<Message, String> {

        private final NestedDataToFlatMapTransformation flatMapper;
        private final ObjectMapper jsonMapper;

        public ProtoToFlattedJsonConverter(String jsonDescription, Message.Builder builder) {
            var conf = NestedDataToFlatMapTransformation.buildMapFromString(jsonDescription);
            this.flatMapper = new NestedDataToFlatMapTransformation(builder.getDescriptorForType(), conf);
            jsonMapper = new ObjectMapper();
//            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT); //pretty print
        }

        @Override
        public String convert(Message input) throws Exception {
            return jsonMapper.writeValueAsString(
                    flatMapper.flattenMapFromProto(input));
        }

    }
}
