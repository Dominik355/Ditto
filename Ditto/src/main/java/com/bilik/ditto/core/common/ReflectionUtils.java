package com.bilik.ditto.core.common;

import com.bilik.ditto.core.exception.ReflectionException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class ReflectionUtils {

    /**
     * Creates an instance using empty constructor of given class
     */
    public static <T> T createInstance(Class<T> clasz) {
        try {
            return accessibleConstructor(clasz).newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            throw new ReflectionException("Internal reflection error. Could not create instance of class " + clasz, ex);
        }
    }

    public static Method getMethod(Class<?> clasz, String methodName) {
        try {
            // Since we are dealing with a Message type, we can call newBuilder()
            return clasz.getMethod(methodName);

        } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Obtain an accessible constructor for the given class and parameters.
     */
    public static <T> Constructor<T> accessibleConstructor(Class<T> clazz, Class<?>... parameterTypes) throws NoSuchMethodException {
        Constructor<T> ctor = clazz.getDeclaredConstructor(parameterTypes);
        makeAccessible(ctor);
        return ctor;
    }

    @SuppressWarnings("deprecation")  // since 9
    public static void makeAccessible(Constructor<?> ctor) {
        if ((!Modifier.isPublic(ctor.getModifiers()) || !Modifier.isPublic(ctor.getDeclaringClass().getModifiers()))
                && !ctor.isAccessible()) {
            ctor.setAccessible(true);
        }
    }

}
