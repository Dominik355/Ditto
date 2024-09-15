package com.bilik.ditto.core.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StringUtils {

    /**
     * Isn't this StringJoiner ???
     * Yes, but it also accepts null values without throwing exception, just does not use them.
     * Collectors.joining() actually uses StringJoiner internally
     */
    public static String joinNullable(CharSequence delimiter, CharSequence... parts) {
        if (parts == null) {
            return "null";
        }
        return Stream.of(parts)
                .filter(s -> s != null && !s.isEmpty())
                .collect(Collectors.joining(delimiter));
    }

    public static boolean isNotBlank(CharSequence str) {
        return !isBlank(str);
    }

    public static boolean isBlank(CharSequence str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((!Character.isWhitespace(str.charAt(i)))) {
                return false;
            }
        }
        return true;
    }

}
