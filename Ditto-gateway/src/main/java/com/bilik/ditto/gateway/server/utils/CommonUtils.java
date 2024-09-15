package com.bilik.ditto.gateway.server.utils;

public class CommonUtils {

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0 || str.trim().length() == 0;
    }

}
