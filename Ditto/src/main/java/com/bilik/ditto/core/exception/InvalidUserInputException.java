package com.bilik.ditto.core.exception;

public class InvalidUserInputException extends DittoCustomException {

    public InvalidUserInputException(String message) {
        super(message);
    }

    public InvalidUserInputException(String message, Throwable cause) {
        super(message, cause);
    }

    public static InvalidUserInputException of(String message, Object... args) {
        return new InvalidUserInputException(DittoCustomException.format(message, args));
    }

    public static InvalidUserInputException of(Throwable throwable, String message, Object... args) {
        return new InvalidUserInputException(DittoCustomException.format(message, args), throwable);
    }

}
