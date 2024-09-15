package com.bilik.ditto.gateway.server;

import com.bilik.ditto.gateway.GlobalExecutors;
import com.bilik.ditto.gateway.server.objects.HttpMethod;
import com.bilik.ditto.gateway.server.utils.ObjectMapperFactory;
import com.bilik.ditto.gateway.server.utils.URLQueryUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class RestServer {

    private static final Logger log = LoggerFactory.getLogger(RestServer.class);
    private HttpServer server;
    private final Map<String, RequestHandler<?>> handlerMap = new HashMap<>();

    public RestServer(int port, int backlog) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), backlog);
        this.server.setExecutor(GlobalExecutors.getInstance().restServerPool());

        this.server.start();
    }

    public <T> void registerHandler(String path, T instance) {
        log.info("Registering RequestHandler of class " + instance.getClass() + " for path " + path);

        path = URLQueryUtils.stripEndingSlashes(path);
        path = URLQueryUtils.stripLeadingSlashes(path);
        path  = "/" + path; // if there are multiple slashes at start, we remove them all and then add 1 to be sure

        if (handlerMap.containsKey(path)) {
            throw new IllegalStateException("RequestHandler for path '" + path + "' has already been defined!");
        }
        try {
            var handler = new RequestHandler<>(path, instance, ObjectMapperFactory.getInstance().getMapper());
            handlerMap.put(path, handler);
            server.createContext(path, handler);
        } catch (Exception ex) {
            log.error("Problem occurred at registering new RequestHandler for path: " + path);
            this.stop();
            throw ex;
        }
    }

    public InetSocketAddress getAddress() {
        return server.getAddress();
    }

    public Set<Pair<String, HttpMethod>> getEndpoints() {
        Set<Pair<String, HttpMethod>> endpoints = new HashSet<>();
        for (var entry : handlerMap.entrySet()) {
            for (var entry2 : entry.getValue().getEndpoints().entrySet()) {
                for (var entry3 : entry2.getValue().entrySet()) {
                    endpoints.add(Pair.of(entry.getKey() + "/" + entry2.getKey(), entry3.getKey()));
                }
            }
        }
        return endpoints;
    }

    public boolean isRunning() {
        return server != null;
    }

    public void stop() {
        stop(1);
    }

    public void stop(int delay) {
        log.info("Stopping server with delay: " + delay);
        try {
            server.stop(delay);

            if (server.getExecutor() instanceof ExecutorService executorService) {
                executorService.shutdown();
            }
        } finally {
            server = null;
        }
    }

}
