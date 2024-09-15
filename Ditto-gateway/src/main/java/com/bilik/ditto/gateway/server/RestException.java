package com.bilik.ditto.gateway.server;

import com.bilik.ditto.gateway.server.objects.HttpStatus;

public class RestException extends RuntimeException {

    private final HttpStatus statusCode;

    public RestException(String message, HttpStatus statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public RestException(String message, Throwable cause, HttpStatus statusCode) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public RestException(Throwable cause, HttpStatus statusCode) {
        super(cause);
        this.statusCode = statusCode;
    }

    public RestException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, HttpStatus statusCode) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.statusCode = statusCode;
    }

    public HttpStatus getStatusCode() {
        return statusCode;
    }
}