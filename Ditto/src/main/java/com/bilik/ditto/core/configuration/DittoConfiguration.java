package com.bilik.ditto.core.configuration;

import com.bilik.ditto.core.configuration.validation.ValidDirectory;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;

@Data
@AllArgsConstructor
@ConfigurationProperties("ditto")
public class DittoConfiguration {

    @ValidDirectory
    private String workingPath;

    private int maxJobsRunning;

    private OrchestrationConfig orchestrationConfig;

    private boolean isLocalInstance;

    private boolean exitOnUnavailableCluster;

    public Path getWorkingPath() {
        return Path.of(workingPath);
    }

    @Data
    public static class OrchestrationConfig {
        /**
         * Queue size between source and worker (throttle used memory)
         */
        private final Integer queueSize;
    }
}
