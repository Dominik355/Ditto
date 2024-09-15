package com.bilik.ditto.core.util;

public class ExceptionUtils {

    public static void rethrowException(Throwable t, String parentMessage) throws Exception {
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof Exception) {
            throw (Exception) t;
        } else {
            throw new Exception(parentMessage, t);
        }
    }

}
