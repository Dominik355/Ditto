package com.bilik.ditto.gateway.server.objects;

import java.util.Optional;
import java.util.StringJoiner;

public class HttpResponse<T> {

    private final HttpHeaders headers;
    private final Optional<T> body;
    private final HttpStatus httpStatus;

    public HttpResponse(HttpHeaders headers, T body, HttpStatus httpStatus) {
        this.headers = headers;
        this.body = Optional.ofNullable(body);
        this.httpStatus = httpStatus;
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

    public Optional<T> getBody() {
        return body;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", HttpResponse.class.getSimpleName() + "[", "]")
                .add("headers=" + headers)
                .add("body=" + body)
                .add("httpStatus=" + httpStatus)
                .toString();
    }
}