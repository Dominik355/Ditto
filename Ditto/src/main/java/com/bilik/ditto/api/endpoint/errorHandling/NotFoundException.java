package com.bilik.ditto.api.endpoint.errorHandling;

import com.bilik.ditto.core.exception.DittoRuntimeException;

public class NotFoundException extends DittoRuntimeException {

    public NotFoundException(String message) {
        super(message);
    }

    public NotFoundException(Throwable cause) {
        super(cause);
    }

    public NotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
