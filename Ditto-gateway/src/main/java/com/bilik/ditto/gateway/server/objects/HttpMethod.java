package com.bilik.ditto.gateway.server.objects;

import java.util.Objects;
import java.util.Optional;

public enum HttpMethod {

    GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS, TRACE;


    public static Optional<HttpMethod> resolve(String method) {
        Objects.requireNonNull(method, "Request method must not be null");
        return Optional.ofNullable(
                switch (method) {
                    case "GET" -> GET;
                    case "HEAD" -> HEAD;
                    case "POST" -> POST;
                    case "PUT" -> PUT;
                    case "PATCH" -> PATCH;
                    case "DELETE" -> DELETE;
                    case "OPTIONS" -> OPTIONS;
                    case "TRACE" -> TRACE;
                    default -> null;
                }
        );
    }

}