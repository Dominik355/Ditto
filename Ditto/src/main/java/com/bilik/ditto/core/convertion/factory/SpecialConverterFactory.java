package com.bilik.ditto.core.convertion.factory;

import java.util.Set;

public interface SpecialConverterFactory<IN, OUT> extends ConverterFactory<IN, OUT> {

    String name();

    Set<String> specificTypes();

    Set<String> args();

}
