package com.bilik.ditto.core.configuration.properties;

import com.bilik.ditto.core.configuration.validation.ValidURI;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@ConfigurationProperties("ditto.s3")
@Data
public class S3Properties {

    private List<Instance> instances = new ArrayList<>();

    @Data
    public static class Instance {
        @NotBlank
        private final String name;
        @ValidURI
        private final String uri;
        private final Properties properties;

        public URI getURI() {
            try {
                return new URI(uri);
            } catch (URISyntaxException e) {
                throw new RuntimeException("Invalid uri: " + uri, e);
            }
        }

        public Properties getProperties() {
            Properties props = new Properties(properties.size());
            props.putAll(properties);
            return props;
        }
    }
}
