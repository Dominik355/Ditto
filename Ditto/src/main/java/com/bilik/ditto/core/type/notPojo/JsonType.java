package com.bilik.ditto.core.type.notPojo;

import com.bilik.ditto.core.type.DataType;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.type.TypeSerde;
import com.bilik.ditto.core.type.pojo.StringType;

public class JsonType extends Type<String> {
    @Override
    public DataType dataType() {
        return DataType.JSON;
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
        return "JSON";
    }

}
