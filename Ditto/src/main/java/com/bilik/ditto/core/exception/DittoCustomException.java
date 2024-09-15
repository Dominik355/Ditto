package com.bilik.ditto.core.exception;

import org.slf4j.helpers.MessageFormatter;

public abstract class DittoCustomException extends RuntimeException {

    protected DittoCustomException() {
        super();
    }

    protected DittoCustomException(String message) {
        super(message);
    }

    protected DittoCustomException(String message, Throwable cause) {
        super(message, cause);
    }

    protected DittoCustomException(Throwable cause) {
        super(cause);
    }

    protected DittoCustomException(String message, Throwable cause, boolean enableSuppression,
                                  boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected DittoCustomException(String stringToFormat, Object... args) {
        super(format(stringToFormat, args));
    }

    /**
     * This just feel nicer than String.format() .. also faster but thats not necessary
     */
    protected static String format(String stringToFormat, Object... args) {
        return MessageFormatter.format(stringToFormat, args).getMessage();
    }
}
