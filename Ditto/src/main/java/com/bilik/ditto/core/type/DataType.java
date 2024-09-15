package com.bilik.ditto.core.type;

import org.apache.commons.lang3.EnumUtils;

import java.util.Optional;

public enum DataType {

    LONG(true),
    INTEGER(true),
    STRING(true),
    UUID(true),
    JSON(false),
    PROTOBUF(false);

    /**
     * Basic types can not be converted to another type, so there is no transition for them
     */
    private final boolean isBasicType;

    public boolean isBasicType() {
        return isBasicType;
    }

    DataType(boolean isBasicType) {
        this.isBasicType = isBasicType;
    }

    public static Optional<DataType> fromString(String typeString) {
        return Optional.ofNullable(EnumUtils.getEnum(DataType.class, typeString));
    }

}