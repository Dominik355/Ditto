package com.bilik.ditto.core.util;

import org.slf4j.helpers.MessageFormatter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class Preconditions {

    // long
    public static long requireBiggerThanZero(long actual) {
        return requireBiggerThan(actual, 0);
    }

    public static long requireBiggerThan(long actual, long value) {
        if (actual <= value) {
            throw new IllegalArgumentException("Actual " + actual + " has to be bigger than " + value);
        }
        return actual;
    }

    // int
    public static int requireBiggerThanZero(int actual) {
        return requireBiggerThan(actual, 0);
    }

    public static int requireBiggerThan(int actual, int value) {
        if (actual <= value) {
            throw new IllegalArgumentException("Actual " + actual + " has to be bigger than " + value);
        }
        return actual;
    }

    // OBJECT

    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }

    public static <T> T checkNotNull(T reference, String errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    public static <T> T checkNotNull(T reference, String errorMessageTemplate, Object... errorMessageArgs) {
        if (reference == null) {
            throw new NullPointerException(format(errorMessageTemplate, errorMessageArgs));
        }
        return reference;
    }

    // STRING

    public static String requireNotBlank(String string) {
        if (StringUtils.isBlank(string)) {
            throw new IllegalArgumentException("String can not be blank !");
        }
        return string;
    }

    public static String requireNotBlank(String string, String message) {
        if (StringUtils.isBlank(string)) {
            throw new IllegalArgumentException(message);
        }
        return string;
    }

    public static String requireNotBlank(String string, String message, Object... args) {
        if (StringUtils.isBlank(string)) {
            throw new IllegalArgumentException(format(message, args));
        }
        return string;
    }

    // ARRAY

    public static void requireNotEmpty(Object[] arr) {
        if (arr == null || arr.length == 0) {
            throw new IllegalArgumentException("Array can not be empty");
        }
    }

    public static void hasElements(Object[] arr, int elementCount) {
        requireNotEmpty(arr);
        if (arr.length != elementCount) {
            throw new IllegalArgumentException("Array does not have " + elementCount + " elements. Actual size: " + arr.length + ".Elements: " + Arrays.asList(arr));
        }
    }


    // COLLECTION

    public static void requireNotEmpty(Collection<?> list) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("Collection can not be null or empty!");
        }
    }

    public static void requireNotEmpty(Collection<?> list, String message) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void requireNotEmpty(Collection<?> list, String message, Object... args) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException(format(message, args));
        }
    }

    // EXPRESSION

    public static void checkState(boolean expression, String message) {
        if (!expression) {
            throw new IllegalStateException(message);
        }
    }


    public static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    public static <T> void requireElement(Collection<T> coll, T element) {
        requireNotEmpty(coll);
        Objects.requireNonNull(element);
        if (!coll.contains(element)) {
            throw new IllegalArgumentException("Provided collection does not contain object: " + element);
        }
    }

    // PRIVATE STUFF

    private static String format (String template, Object... args) {
        return MessageFormatter.format(template, args).getMessage();
    }

}
