package com.bilik.ditto.core.convertion.factory;

import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.convertion.Converter;
import com.bilik.ditto.core.type.DataType;

public interface ConverterFactory<IN, OUT> {

    Converter<IN, OUT> createConverter(JobDescriptionInternal jobDescription);

    DataType inputType();

    DataType outputType();

}
