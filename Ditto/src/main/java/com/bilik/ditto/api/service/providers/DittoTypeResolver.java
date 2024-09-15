package com.bilik.ditto.api.service.providers;

import com.bilik.ditto.api.domain.dto.JobDescriptionDto;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.exception.InvalidUserInputException;
import com.bilik.ditto.core.type.DataType;
import com.bilik.ditto.core.type.SupportedPlatform;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.type.notPojo.JsonType;
import com.bilik.ditto.core.type.pojo.IntegerType;
import com.bilik.ditto.core.type.pojo.LongType;
import com.bilik.ditto.core.type.pojo.StringType;
import com.bilik.ditto.core.type.pojo.UUIDType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class DittoTypeResolver {

    public static final Map<SupportedPlatform, Set<DataType>> supportedTypesByPlatform = Map.of(
            SupportedPlatform.KAFKA, EnumSet.allOf(DataType.class),
            SupportedPlatform.S3, EnumSet.of(DataType.PROTOBUF, DataType.JSON),
            SupportedPlatform.LOCAL_FS, EnumSet.of(DataType.PROTOBUF, DataType.JSON),
            SupportedPlatform.HDFS, EnumSet.of(DataType.PROTOBUF, DataType.JSON)
    );

    private final ProtobufTypeResolver protobufTypeResolver;

    @Autowired
    public DittoTypeResolver(ProtobufTypeResolver protobufTypeResolver) {
        this.protobufTypeResolver = protobufTypeResolver;
    }

    public Map<String, List<String>> getSupportedTypes() {
        Map<String, List<String>> types = new HashMap<>();
        types.put("long", null);
        types.put("int", null);
        types.put("string", null);
        types.put("uuid", null);
        types.put("json", null);
        types.put("protobuf", List.copyOf(protobufTypeResolver.availableProtos()));
        return types;
    }

    public List<String> getSupportedProtos() {
        return getSupportedTypes().get("protobuf");
    }

    public Type getDataType(JobDescriptionDto.DataType providedType) {
        return switch (providedType.type()) {
            case "long" -> new LongType();
            case "int" -> new IntegerType();
            case "string" -> new StringType();
            case "uuid" -> new UUIDType();
            case "json" -> new JsonType();
            case "protobuf" -> protobufTypeResolver.getType(providedType.specificType())
                    .orElseThrow(() -> new IllegalArgumentException("Protobuf type for [" + providedType + "] is not registered"));
            default -> throw new IllegalStateException("Not supported data type: " + providedType.type());
        };
    }

    public JobDescriptionInternal.SourceGeneral resolveSource(JobDescriptionDto.SourceGeneral dto) {
        SupportedPlatform sp = SupportedPlatform.fromString(dto.platform())
                .orElseThrow(() -> InvalidUserInputException.of("Platform {} does not exists"));

        if (!sp.supportsSource()) {
            throw InvalidUserInputException.of("Platform {} doesn't have implemented Source part", sp);
        }

        Type dataType = getDataType(dto.dataType());

        if (!supportedTypesByPlatform.get(sp).contains(dataType.dataType())) {
            throw InvalidUserInputException.of("Source type {} does not support Data Type {}", sp, dataType.name());
        }

        return new JobDescriptionInternal.SourceGeneral(dataType, sp);
    }

    public JobDescriptionInternal.SinkGeneral resolveSink(JobDescriptionDto.SinkGeneral dto) {
        SupportedPlatform sp = SupportedPlatform.fromString(dto.platform())
                .orElseThrow(() -> InvalidUserInputException.of("Platform {} does not exists"));

        if (!sp.supportsSink()) {
            throw InvalidUserInputException.of("Platform {} doesn't have implemented Sink part", sp);
        }

        Type dataType = getDataType(dto.dataType());

        if (!supportedTypesByPlatform.get(sp).contains(dataType.dataType())) {
            throw InvalidUserInputException.of("Sink type {} does not support Data Type {}", sp, dataType.name());
        }

        return new JobDescriptionInternal.SinkGeneral(dataType, sp);
    }

}
