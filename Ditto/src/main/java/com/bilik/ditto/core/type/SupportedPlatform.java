package com.bilik.ditto.core.type;

import org.apache.commons.lang3.EnumUtils;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum SupportedPlatform {
    
    S3(true, true),
    KAFKA(true, true),
    LOCAL_FS(false, true),
    HDFS(true, true);
    
    private final boolean supportedSource;
    private final boolean supportedSink;

    public boolean supportsSink() {
        return supportedSink;
    }

    public boolean supportsSource() {
        return supportedSource;
    }

    SupportedPlatform(boolean supportedSource, boolean supportedSink) {
        this.supportedSource = supportedSource;
        this.supportedSink = supportedSink;
    }

    public static Optional<SupportedPlatform> fromString(String typeString) {
        return Optional.ofNullable(EnumUtils.getEnum(SupportedPlatform.class, typeString));
    }

    public static Map<String, SupportedPlatform> getSupportedSourceTypes() {
        return getSupportedTypes(SupportedPlatform::supportsSource);
    }

    public static Map<String, SupportedPlatform> getSupportedSinkTypes() {
        return getSupportedTypes(SupportedPlatform::supportsSink);
    }

    private static Map<String, SupportedPlatform> getSupportedTypes(Predicate<SupportedPlatform> predicate) {
        return Stream.of(SupportedPlatform.values())
                .filter(predicate)
                .collect(Collectors.toMap(
                        SupportedPlatform::name,
                        Function.identity()
                ));
    }

}
