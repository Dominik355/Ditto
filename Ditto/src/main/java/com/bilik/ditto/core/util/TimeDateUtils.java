package com.bilik.ditto.core.util;

public class TimeDateUtils {

    public static boolean isSameSecond(long v1, long v2) {
        return Math.floorDiv(v1, 1000) == Math.floorDiv(v2, 1000);
    }

}
