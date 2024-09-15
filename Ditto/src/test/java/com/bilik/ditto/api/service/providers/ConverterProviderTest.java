package com.bilik.ditto.api.service.providers;

import com.bilik.proto.video.ad.VideoAd;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.core.convertion.factory.JsonToProtoConverterFactory;
import com.bilik.ditto.core.convertion.factory.ProtoToFlattedJsonConverterFactory;
import com.bilik.ditto.core.convertion.factory.ProtoToJsonConverterFactory;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.type.SupportedPlatform;
import com.bilik.ditto.core.type.notPojo.JsonType;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType;
import com.bilik.ditto.core.type.pojo.IntegerType;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ConverterProviderTest {

    static final String DATA_TYPE = "video";

    @Test
    void missingSourceInfo() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSourceGeneral(
                new JobDescriptionInternal.SourceGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> ConverterProvider.getConverterSupplier(jobDescriptionInternal));
    }

    @Test
    void missingSinkInfo() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSinkGeneral(
                new JobDescriptionInternal.SinkGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> ConverterProvider.getConverterSupplier(jobDescriptionInternal));
    }

    @Test
    void transformNotAvailableCombination() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSourceGeneral(
                new JobDescriptionInternal.SourceGeneral(
                        new IntegerType(),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setSinkGeneral(
                new JobDescriptionInternal.SinkGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() -> ConverterProvider.getConverterSupplier(jobDescriptionInternal));
    }

    @Test
    void defaultConverterProtoToJson() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSourceGeneral(
                new JobDescriptionInternal.SourceGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setSinkGeneral(
                new JobDescriptionInternal.SinkGeneral(
                        new JsonType(),
                        SupportedPlatform.KAFKA
                )
        );

        var converterSupplier = ConverterProvider.getConverterSupplier(jobDescriptionInternal);
        assertThat(converterSupplier).isNotNull();
        assertThat(converterSupplier.get().getClass()).isEqualTo(ProtoToJsonConverterFactory.ProtoToJsonConverter.class);
    }

    @Test
    void defaultConverterJsonToProto() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSourceGeneral(
                new JobDescriptionInternal.SourceGeneral(
                        new JsonType(),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setSinkGeneral(
                new JobDescriptionInternal.SinkGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );

        var converterSupplier = ConverterProvider.getConverterSupplier(jobDescriptionInternal);
        assertThat(converterSupplier).isNotNull();
        assertThat(converterSupplier.get().getClass()).isEqualTo(JsonToProtoConverterFactory.JsonToProtoConverter.class);
    }

    @Test
    void specialConverter_notRegisteredName() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSourceGeneral(
                new JobDescriptionInternal.SourceGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setSinkGeneral(
                new JobDescriptionInternal.SinkGeneral(
                        new JsonType(),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setConverter(
                new JobDescriptionInternal.Converter(
                        "non existing",
                        null,
                        null
                )
        );

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() -> ConverterProvider.getConverterSupplier(jobDescriptionInternal))
                .withMessage("Converter with name [non existing] was not found");
    }

    @Test
    void specialConverter_protoToFlatJson_noSpecificType() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSourceGeneral(
                new JobDescriptionInternal.SourceGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setSinkGeneral(
                new JobDescriptionInternal.SinkGeneral(
                        new JsonType(),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setConverter(
                new JobDescriptionInternal.Converter(
                        ProtoToFlattedJsonConverterFactory.NAME,
                        null,
                        null
                )
        );

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> ConverterProvider.getConverterSupplier(jobDescriptionInternal))
                .withMessage("Invalid specific type");
    }

    @Test
    void specialConverter_protoToFlatJson_nonExistingSpecificType() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSourceGeneral(
                new JobDescriptionInternal.SourceGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setSinkGeneral(
                new JobDescriptionInternal.SinkGeneral(
                        new JsonType(),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setConverter(
                new JobDescriptionInternal.Converter(
                        ProtoToFlattedJsonConverterFactory.NAME,
                        "non existing",
                        null
                )
        );

        assertThatExceptionOfType(DittoRuntimeException.class)
                .isThrownBy(() -> ConverterProvider.getConverterSupplier(jobDescriptionInternal))
                .withMessage("ProtoToFlattedJsonConverter for schema [non existing] was not found");
    }

    @Test
    void specialConverter_protoToFlatJson_correct() {
        JobDescriptionInternal jobDescriptionInternal = new JobDescriptionInternal();
        jobDescriptionInternal.setSourceGeneral(
                new JobDescriptionInternal.SourceGeneral(
                        new ProtobufType<>(Sensor.class),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setSinkGeneral(
                new JobDescriptionInternal.SinkGeneral(
                        new JsonType(),
                        SupportedPlatform.KAFKA
                )
        );
        jobDescriptionInternal.setConverter(
                new JobDescriptionInternal.Converter(
                        ProtoToFlattedJsonConverterFactory.NAME,
                        "Sensor_flatmap",
                        null
                )
        );

        var converterSupplier = ConverterProvider.getConverterSupplier(jobDescriptionInternal);
        assertThat(converterSupplier).isNotNull();
        assertThat(converterSupplier.get().getClass()).isEqualTo(ProtoToFlattedJsonConverterFactory.ProtoToFlattedJsonConverter.class);
    }

}
