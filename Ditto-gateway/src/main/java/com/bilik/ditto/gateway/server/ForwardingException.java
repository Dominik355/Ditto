package com.bilik.ditto.gateway.server;

import com.bilik.ditto.gateway.dto.ApiError;
import com.bilik.ditto.gateway.server.objects.HttpStatus;

/**
 * Purpose of this is to forward exceptions thrown by Ditto to FE
 */
public class ForwardingException extends RestException {

    private final ApiError apiError;

    public ForwardingException(ApiError apiError) {
        super(apiError.getError(), HttpStatus.valueOf(apiError.getStatus()));
        this.apiError = apiError;
    }

    public ForwardingException(Throwable cause, ApiError apiError) {
        super(apiError.getError(), cause, HttpStatus.valueOf(apiError.getStatus()));
        this.apiError = apiError;
    }

    public ForwardingException(Throwable cause, boolean enableSuppression, boolean writableStackTrace, ApiError apiError) {
        super(apiError.getError(), cause, enableSuppression, writableStackTrace, HttpStatus.valueOf(apiError.getStatus()));
        this.apiError = apiError;
    }

    public ApiError getApiError() {
        return apiError;
    }
}
