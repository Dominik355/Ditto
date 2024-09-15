package com.bilik.ditto.core.exception;

public class DittoRuntimeException extends DittoCustomException {

    public DittoRuntimeException(String message) {
        super(message);
    }

    public DittoRuntimeException(Throwable cause) {
        super(cause);
    }

    public DittoRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public static DittoRuntimeException of(String message, Object... args) {
        return new DittoRuntimeException(format(message, args));
    }

    public static DittoRuntimeException of(Throwable throwable, String message, Object... args) {
        return new DittoRuntimeException(format(message, args), throwable);
    }
    
}
