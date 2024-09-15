package com.bilik.ditto.api.service.providers;

import com.bilik.ditto.api.domain.dto.ConverterDto;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.convertion.Converter;
import com.bilik.ditto.core.convertion.factory.ConverterFactory;
import com.bilik.ditto.core.convertion.factory.JsonToProtoConverterFactory;
import com.bilik.ditto.core.convertion.factory.ProtoToFlattedJsonConverterFactory;
import com.bilik.ditto.core.convertion.factory.ProtoToJsonConverterFactory;
import com.bilik.ditto.core.convertion.factory.SpecialConverterFactory;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.type.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class ConverterProvider {

    private static final Logger log = LoggerFactory.getLogger(ConverterProvider.class);

    private static final Set<ConverterFactory<?, ?>> defaultConverters = Set.of(
            new ProtoToJsonConverterFactory(),
            new JsonToProtoConverterFactory()
    );

    private static final Set<SpecialConverterFactory<?, ?>> specialConverters = Set.of(
            new ProtoToFlattedJsonConverterFactory(),
    );

    public static Supplier<Converter<?, ?>> getConverterSupplier(JobDescriptionInternal jobDescription) {
        if (jobDescription.getConverter() != null) {
            return getSpecialConverterSupplier(jobDescription);
        } else {
            return getDefaultConverterSupplier(jobDescription);
        }
    }

    private static Supplier<Converter<?, ?>> getSpecialConverterSupplier(JobDescriptionInternal jobDescription) {
        log.info("Choosing special converter for job {}", jobDescription.getJobId());
        SpecialConverterFactory<?, ?> factory = specialConverters.stream()
                .filter(fct -> fct.name().equals(jobDescription.getConverter().getName()))
                .findFirst()
                .orElseThrow(() -> DittoRuntimeException.of("Converter with name {} was not found", jobDescription.getConverter().getName()));

        if (!factory.inputType().equals(jobDescription.getSourceGeneral().getDataType().dataType()) ||
                !factory.outputType().equals(jobDescription.getSinkGeneral().getDataType().dataType())) {
            throw new IllegalArgumentException("Special converter " + factory.name() + " is not supported for " +
                    "source[" + jobDescription.getSourceGeneral().getDataType().dataType() + "] and " +
                    "sink[" + jobDescription.getSinkGeneral().getDataType().dataType() + "] type." +
                    "Desired data types are: " + factory.inputType() + " -> " + factory.outputType());
        }
        factory.createConverter(jobDescription); // if any exception should be thrown at instance creation, do so now
        return () -> factory.createConverter(jobDescription);
    }

    private static Supplier<Converter<?, ?>> getDefaultConverterSupplier(final JobDescriptionInternal jobDescription) {
        log.info("Choosing default converter for job {}", jobDescription.getJobId());

        DataType sourceType = jobDescription.getSourceGeneral().getDataType().dataType();
        DataType sinkType = jobDescription.getSinkGeneral().getDataType().dataType();

        return defaultConverters.stream()
                .filter(converter -> converter.inputType().equals(sourceType) && converter.outputType().equals(sinkType))
                .findFirst()
                .<Supplier<Converter<?, ?>>>map(factory -> () -> factory.createConverter(jobDescription))
                .orElseThrow(() -> DittoRuntimeException.of("Transition from {} to {} is not supported", sourceType.name(), sinkType.name()));
    }

    public static List<ConverterDto> getSupportedConverters() {
        return specialConverters.stream()
                .map(factory -> new ConverterDto(
                        factory.name(),
                        factory.specificTypes(),
                        factory.args()
                ))
                .toList();
    }

}
