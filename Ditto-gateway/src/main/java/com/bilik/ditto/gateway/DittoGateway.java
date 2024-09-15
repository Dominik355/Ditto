package com.bilik.ditto.gateway;

import com.bilik.ditto.gateway.clients.DittoClient;
import com.bilik.ditto.gateway.clients.HttpClient;
import com.bilik.ditto.gateway.clients.KubeClient;
import com.bilik.ditto.gateway.endpoints.ActuatorEndpoint;
import com.bilik.ditto.gateway.endpoints.InfoEndpoint;
import com.bilik.ditto.gateway.endpoints.JobEndpoint;
import com.bilik.ditto.gateway.endpoints.WelcomeEndpoint;
import com.bilik.ditto.gateway.server.RestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DittoGateway {

    private static final Logger log = LoggerFactory.getLogger(DittoGateway.class);

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        RestServer server = new RestServer(config.port, config.backlog);

        DittoService dittoService = new DittoService(
                new DittoClient(new HttpClient()),
                new KubeClient(
                        config.kubeNamespace,
                        config.dittoServiceName,
                        "ditto",
                        null
                ));

        server.registerHandler("/api/v1/info", new InfoEndpoint(dittoService));
        server.registerHandler("/api/v1/job", new JobEndpoint(dittoService));
        server.registerHandler("/actuator/health", new ActuatorEndpoint());
        server.registerHandler("/", new WelcomeEndpoint(dittoService));

        log.info("Available endpoints: {}", server.getEndpoints().stream().map(pair -> pair.toString()).reduce((a, b) -> a + ",\n" + b).get());
        printBanner();
    }

    private static void printBanner() {
        var version = DittoGateway.class.getPackage().getImplementationVersion(); // takes from manifest inside jar

        log.info("""
                    
                    
                    ____  _ __  __           ______      __                         \s
                   / __ \\(_) /_/ /_____     / ____/___ _/ /____ _      ______ ___  __
                  / / / / / __/ __/ __ \\   / / __/ __ `/ __/ _ \\ | /| / / __ `/ / / /
                 / /_/ / / /_/ /_/ /_/ /  / /_/ / /_/ / /_/  __/ |/ |/ / /_/ / /_/ /\s
                /_____/_/\\__/\\__/\\____/   \\____/\\__,_/\\__/\\___/|__/|__/\\__,_/\\__, / \s
                                                                            /____/ \s
                                           ... HAS STARTED  | Version: {}
                                           
                """, version != null ? version : "DEVELOPMENT");

    }

}