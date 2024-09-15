package com.bilik.ditto.core.convertion;

import com.bilik.ditto.core.job.StreamElement;

public interface Converter<IN, OUT> {

    OUT convert(IN input) throws Exception;

    @SuppressWarnings("unchecked")
    default StreamElement<OUT> convertLastElement(StreamElement<IN> input) throws Exception {
        return (StreamElement<OUT>) input;
    };

}
