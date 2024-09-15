package com.bilik.ditto.core.configuration.properties;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@Data
@ConfigurationProperties("ditto.hdfs")
public class HdfsProperties {

    private List<HdfsProperties.Cluster> clusters = new ArrayList<>();

    @Data
    public static class Cluster {
        @NotBlank
        private final String name;
        @NotBlank
        private final String uri;
    }

}
