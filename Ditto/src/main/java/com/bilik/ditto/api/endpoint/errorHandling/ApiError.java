package com.bilik.ditto.api.endpoint.errorHandling;

import lombok.Getter;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;

@Getter
public class ApiError {

    private final LocalDateTime timestamp;
    private final int status;
    private final String error;
    private final String path;

    public ApiError(HttpStatus status, String error, String path) {
        this.timestamp = LocalDateTime.now();
        this.status = status.value();
        this.error = error;
        this.path = path;
    }

}