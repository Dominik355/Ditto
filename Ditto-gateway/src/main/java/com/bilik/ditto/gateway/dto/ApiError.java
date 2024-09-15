package com.bilik.ditto.gateway.dto;

import com.bilik.ditto.gateway.server.objects.HttpStatus;

import java.time.LocalDateTime;
import java.util.StringJoiner;

public class ApiError {

    private LocalDateTime timestamp;
    private int status;
    private String error;
    private String path;

    public ApiError() {}

    public ApiError(int status, String error, String path) {
        this.timestamp = LocalDateTime.now();
        this.status = status;
        this.error = error;
        this.path = path;
    }

    public static ApiError internalServerError(String path) {
        return new ApiError(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.reasonPhrase(),
                path);
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public int getStatus() {
        return status;
    }

    public String getError() {
        return error;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ApiError.class.getSimpleName() + "[", "]")
                .add("timestamp=" + timestamp)
                .add("status=" + status)
                .add("error='" + error + "'")
                .add("path='" + path + "'")
                .toString();
    }
}