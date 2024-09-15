package com.bilik.ditto.gateway.server;

import com.bilik.ditto.gateway.server.objects.HttpMethod;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RequestMapping {
    /**
     * Mapping for the annotated method.
     */
    String value() default "";

    HttpMethod method() default HttpMethod.GET;
}