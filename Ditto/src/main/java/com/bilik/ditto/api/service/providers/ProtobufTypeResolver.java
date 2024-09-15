package com.bilik.ditto.api.service.providers;

import com.google.protobuf.Message;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufClassesScanner;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufType;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ProtobufTypeResolver {

    private static final Logger log = LoggerFactory.getLogger(ProtobufTypeResolver.class);

    private Map<String, ProtobufRegisterInfo> registeredProtos;

    public ProtobufTypeResolver() {
        init();
    }

    public void init() {
        registeredProtos =  ProtobufClassesScanner.getRegisteredProtos()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                        ProtobufRegisterInfo::name,
                        Function.identity()));

        log.info("ProtobufTypeResolver has registered {} protos", registeredProtos.size());
    }

    public Set<String> availableProtos() {
        return registeredProtos.keySet();
    }

    public Optional<ProtobufType<Message>> getType(String name) {
        ProtobufRegisterInfo info = registeredProtos.get(name);
        if (info != null) {
            return Optional.of(new ProtobufType<>(info));
        }
        return Optional.empty();
    }

    @Builder
    public record ProtobufRegisterInfo (
            String name,
            Class<? extends Message> clasz) {}

}
