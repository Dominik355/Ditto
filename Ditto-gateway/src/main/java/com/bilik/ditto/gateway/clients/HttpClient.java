package com.bilik.ditto.gateway.clients;

import com.bilik.ditto.gateway.GlobalExecutors;
import com.bilik.ditto.gateway.dto.ApiError;
import com.bilik.ditto.gateway.server.ForwardingException;
import com.bilik.ditto.gateway.server.objects.HttpHeaders;
import com.bilik.ditto.gateway.server.objects.HttpResponse;
import com.bilik.ditto.gateway.server.objects.HttpStatus;
import com.fasterxml.jackson.databind.ObjectReader;
import com.bilik.ditto.gateway.server.utils.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class HttpClient {

    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final java.net.http.HttpClient httpClient;
    private final ObjectReader objectReader;

    public HttpClient() {
        this.objectReader = ObjectMapperFactory.reader();
        this.httpClient = java.net.http.HttpClient.newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_2)
                .followRedirects(java.net.http.HttpClient.Redirect.NORMAL)
                .executor(GlobalExecutors.getInstance().httpClientPool())
                .connectTimeout(TIMEOUT)
                .build();
    }

    public <T> CompletableFuture<HttpResponse<T>> send(HttpRequest request, Class<? super T> clasz) {
        log.info("Sending request [{} | {}]", request.method(), request.uri());
        return httpClient.sendAsync(request, java.net.http.HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    try {
                        if (HttpStatus.valueOf(response.statusCode()).isError()) {
                            log.error("Obtained error response from ditto instance: {}, body: {}", response, response.body());
                            throw new ForwardingException(objectReader.readValue(response.body(), ApiError.class));
                        }

                        T deserBody = null;
                        log.debug("Response body for request {}: {}", request.uri(), response.body());
                        if (clasz.equals(String.class)) {
                            deserBody = (T) response.body(); // objectreader cutted strings, so don't use it when String is desired
                        } else if (response.body() == null || response.body().isEmpty()) {
                            // keep null
                        } else {
                            deserBody = (T) objectReader.readValue(response.body(), clasz);
                        }
                        return new HttpResponse<>(
                                new HttpHeaders(response.headers().map()),
                                deserBody,
                                HttpStatus.valueOf(response.statusCode())
                        );
                    } catch (Exception e) {
                        log.error("JSON processing exception: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
    }


}
