package com.bilik.ditto.gateway.server.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class ObjectMapperFactory {

    private final ObjectMapper objectMapper;

    private ObjectMapperFactory() {}

    {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule()); // java8 time
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false); // so it's not written as array and don't have to use annotation in every object with LocalDate[Time]
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT); // pretty print
    }

    public ObjectReader getReader() {
        return objectMapper.reader();
    }

    public ObjectWriter getWriter() {
        return objectMapper.writer();
    }

    public ObjectMapper getMapper() {
        return objectMapper;
    }

    public static ObjectReader reader() {
        return getInstance().getReader();
    }

    public static ObjectWriter writer() {
        return getInstance().getWriter();
    }

    private static class LazySingleton {
        static final ObjectMapperFactory instance = new ObjectMapperFactory();
    }

    public static ObjectMapperFactory getInstance() {
        return LazySingleton.instance;
    }

}
