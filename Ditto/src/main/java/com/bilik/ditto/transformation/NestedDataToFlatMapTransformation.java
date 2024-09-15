package com.bilik.ditto.transformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.bilik.ditto.transformation.vw.TransformationDefinition;
import com.bilik.ditto.transformation.vw.TransformationProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class NestedDataToFlatMapTransformation {

    private final Map<String, ProtoMapperWithTransformation> mapping;

    /**
     * @param descriptor - proto message descriptor
     * @param mapping - configuration Map describing flatting. Key is flatted name and ConfigItem describes mapping
     */
    public NestedDataToFlatMapTransformation(Descriptors.Descriptor descriptor, Map<String, ConfigItem> mapping) {
        this.mapping = new HashMap<>();
        for (Map.Entry<String, ConfigItem> entry: mapping.entrySet()) {
            this.mapping.put(entry.getKey(),
                    new ProtoMapperWithTransformation(
                            new ProtoMapper(descriptor, entry.getValue().name),
                            new MapMapper(entry.getValue().name),
                            entry.getValue().transformation,
                            entry.getValue().defaultValue));
        }
    }

    public NestedDataToFlatMapTransformation(Descriptors.Descriptor descriptor, String jsonConfigSchema) {
        this(descriptor, buildMapFromString(jsonConfigSchema));
    }

    public static Map<String, ConfigItem> buildMapFromString(String jsonConfigSchema) {
        Map<String, JSONObject> pre = JSON.parseObject(jsonConfigSchema, HashMap.class);
        Map<String, ConfigItem> cfg = new HashMap<>(pre.size());
        for (Map.Entry<String, JSONObject> entry: pre.entrySet()) {
            cfg.put(entry.getKey(), new ConfigItem(entry.getValue()));
        }
        return cfg;
    }

    public Map<String, Object> flattenMapFromProto(Message proto) {
        Map<String, Object> result = new HashMap<>(mapping.size());
        for (Map.Entry<String, ProtoMapperWithTransformation> entry: mapping.entrySet()) {
            ProtoMapperWithTransformation protoMapperWithTransformation = entry.getValue();
            Object value = entry.getValue().protoMapper.getValue(proto);

            if (protoMapperWithTransformation.transformationMethod != null) {
                // todo: tmp hack this will be needed better solution
                // like parsing repeatable field once and do every multiple operation basically expand repeatable
                if (protoMapperWithTransformation.selectors != null) {
                    Object[] selectValue = new Object[]{protoMapperWithTransformation.selectors.split("="), value};
                    value = protoMapperWithTransformation.transformationMethod.apply(proto, selectValue);
                } else {
                    value = protoMapperWithTransformation.transformationMethod.apply(proto, value);
                }
            }
            result.put(entry.getKey(), value);
        }
        return result;
    }

    public Map<String, Object> flattenMapFromMap(Map<String, Object> message) {
        HashMap<String, Object> result = new HashMap<>(mapping.size());
        for (Map.Entry<String, ProtoMapperWithTransformation> entry: mapping.entrySet()) {
            ProtoMapperWithTransformation protoMapperWithTransformation = entry.getValue();
            Object value = entry.getValue().mapMapper.getValue(message);
            if (protoMapperWithTransformation.transformationMethod != null) {
                // todo: tmp hack this will be needed better solution
                // like parsing repeatable field once and do every multiple operation basically expand repeatable
                if (protoMapperWithTransformation.selectors != null) {
                    Object[] selectValue = new Object[]{protoMapperWithTransformation.selectors.split("="), value};
                    value = protoMapperWithTransformation.transformationMethod.apply(message, selectValue);
                } else {
                    value = protoMapperWithTransformation.transformationMethod.apply(message, value);
                }
            }
            result.put(entry.getKey(), value);
        }
        return result;
    }

    private static class ProtoMapperWithTransformation {

        public final ProtoMapper protoMapper;
        public final MapMapper mapMapper;
        public final BiFunction transformationMethod;
        public final String selectors;
        public final Object defaultValue;

        public ProtoMapperWithTransformation(
                ProtoMapper protoMapper,
                MapMapper mapMapper,
                TransformationDefinition transformation,
                Object defaultValue)  {
            this.protoMapper = protoMapper;
            this.mapMapper = mapMapper;
            this.defaultValue = defaultValue;
            if (transformation != null) {
                this.transformationMethod = TransformationProvider.getInstance()
                        .getTransformFunction(transformation.getFunction())
                        .andThen(result -> result == null && defaultValue != null ? defaultValue : result);
                this.selectors = transformation.getSelectors();
            } else {
                this.transformationMethod = null;
                this.selectors = null;
            }
        }
    }
}
