package com.bilik.ditto.core.type;

public abstract class Type<T> {

    public abstract DataType dataType();

    public abstract Class<T> getTypeClass();

    public abstract TypeSerde<T> createSerde();

    public abstract String name();

    @Override
    public String toString() {
        return "Type[" + dataType() + ", " + getTypeClass() + "]";
    }
}
