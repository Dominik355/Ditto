package com.bilik.ditto.gateway.server;

import com.bilik.ditto.gateway.dto.ApiError;
import com.bilik.ditto.gateway.server.javaNet.Converter;
import com.bilik.ditto.gateway.server.objects.HttpHeaders;
import com.bilik.ditto.gateway.server.objects.HttpMethod;
import com.bilik.ditto.gateway.server.objects.HttpRequest;
import com.bilik.ditto.gateway.server.objects.HttpResponse;
import com.bilik.ditto.gateway.server.objects.HttpStatus;
import com.bilik.ditto.gateway.server.utils.CommonUtils;
import com.bilik.ditto.gateway.server.utils.URLQueryUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bilik.ditto.gateway.server.objects.HttpStatus.BAD_REQUEST;
import static com.bilik.ditto.gateway.server.objects.HttpStatus.NOT_FOUND;

public class RequestHandler<T> implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    private static final byte[] EMPTY_BODY = new byte[0];

    private final String parentPath;
    private final T parentInstance;
    private final ObjectMapper objectMapper;
    private final Map<String, Map<HttpMethod, SingleEndpoint<?>>> endpoints;

    public RequestHandler(String parentPath,
                          T parentInstance,
                          ObjectMapper objectMapper) {
        this.parentPath = parentPath;
        this.parentInstance = parentInstance;
        this.objectMapper = objectMapper;
        this.endpoints = createMappings();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        HttpRequest request = Converter.from(exchange);

        HttpResponse<?> response;
        try {
            response = invokeEndpointMethod(request);
        } catch (Throwable t) {
            Throwable rootCause = ExceptionUtils.getRootCause(t);
            if (RestException.class.isAssignableFrom(rootCause.getClass())) {
                log.info("RestException occurred: " + rootCause.getMessage());
                RestException ex = (RestException) rootCause;
                response = ex instanceof ForwardingException forwardEx ?
                        createErrorResponse(forwardEx.getMessage(), forwardEx.getStatusCode(), exchange.getRequestURI().getPath()) :
                        createErrorResponse(ex.getMessage(), ex.getStatusCode(), exchange.getRequestURI().getPath());
            } else {
                log.error("Exception of type {}, {}", rootCause.getClass(), ExceptionUtils.getStackTrace(rootCause));
                response = createErrorResponse(null, HttpStatus.INTERNAL_SERVER_ERROR, exchange.getRequestURI().getPath());
            }
        }

        exchange.getResponseHeaders().put(HttpHeaders.CONTENT_TYPE, List.of("application/json")); // we only return json, or primitives/Strings, but should work on those also

        // update to jdk 20 and make it nice with Switch pattern matching
        byte[] responseBody = response.getBody()
                .map(value -> {
                    if (value instanceof String s) { //if body is String, we assume it is already formatted,
                        return s.getBytes(StandardCharsets.UTF_8);
                    } else if (value instanceof byte[] b) { //if body is byte[], just pass it as it is
                        return b;
                    } else {
                        try {
                            return objectMapper.writeValueAsBytes(value);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .orElse(EMPTY_BODY);

        exchange.getResponseHeaders().putAll(response.getHeaders());
        exchange.sendResponseHeaders(response.getHttpStatus().value(), responseBody.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBody);
        }
    }

    private HttpResponse<?> invokeEndpointMethod(HttpRequest request) throws InvocationTargetException, IllegalAccessException {
        String pathWithinClass = extractSpecificPart(request);
        var map = endpoints.get(pathWithinClass);
        if (map == null) {
            throw new RestException("Path " + request.getPath() + " not found.", NOT_FOUND);
        }

        SingleEndpoint<?> endpoint = map.get(request.getMethod());
        if (endpoint == null) {
            throw new RestException("Method " + request.getMethod() + " is not supported for path " + request.getPath(), BAD_REQUEST);
        }

        final Parameter[] parameters = endpoint.method.getParameters();
        Object[] methodArgs = new Object[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
            final Parameter p = parameters[i];

            final RequestBody bodyParam = p.getDeclaredAnnotation(RequestBody.class);
            final RequestParam pathParam = p.getDeclaredAnnotation(RequestParam.class);

            if (pathParam != null) {
                var paramName = pathParam.value();
                if (paramName == null) {
                    throw new RuntimeException("RequestParameter annotation is missing value on method " + endpoint.method.getName());
                }
                var paramVal = request.getParams().get(paramName);
                if (paramVal == null) {
                    if (pathParam.required()) {
                        throw new RestException("Required parameter with name '" + paramName + " is missing", BAD_REQUEST);
                    } else if (p.getType().isPrimitive()) {
                        throw new IllegalStateException("Parameter with name '" + paramName + " is missing. Required type is primitive, which can not be supplied with null");
                    } else {
                        methodArgs[i] = null;
                    }
                } else {
                    methodArgs[i] = objectMapper.convertValue(paramVal, p.getType());
                }
            } else if (bodyParam != null) {
                if (request.getBody() == null) {
                    if (bodyParam.required()) {
                        throw new RestException("Request does not contain required body", BAD_REQUEST);
                    } else if (p.getType().isPrimitive()) {
                        throw new IllegalStateException("Request body is missing. Required primitive type can not be supplied with null");
                    } else {
                        methodArgs[i] = null;
                    }
                } else {
                    try {
                        methodArgs[i] = objectMapper.readValue(request.getBody(), p.getType());
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Could not map given JSON body to object " + p.getType() + ". Root exception is: " + e.getMessage(), e);
                    }
                }
            } else if (p.getType().equals(HttpRequest.class)) {
                methodArgs[i] = request;
            } else if (p.getType().equals(HttpHeaders.class)) {
                methodArgs[i] = request.getHeaders();
            }
        }

        Object result = endpoint.method.invoke(endpoint.parentClassInstance, methodArgs);

        if (endpoint.method.getReturnType().equals(void.class) || endpoint.method.getReturnType().equals(Void.class)) {
            return new HttpResponse<>(HttpHeaders.NO_HEADERS, null, HttpStatus.OK);
        } else if (result instanceof HttpResponse<?> httpResponse) {
            return httpResponse;
        } else {
            return new HttpResponse<>(HttpHeaders.NO_HEADERS, result, HttpStatus.OK);
        }
    }

    private String extractSpecificPart(HttpRequest request) {
        if (request.getPath().length() > 1) {
            if (parentPath.length() == 1) {
                return request.getPath().substring(1);
            } else if (parentPath.equals(request.getPath()) || (parentPath + "/").equals(request.getPath())) {
                return null;
            } else {
                return request.getPath().substring(parentPath.length() + 1);
            }
        } else {
            return null;
        }
    }

    private HttpResponse<ApiError> createErrorResponse(final String message, HttpStatus statusCode, String path) {
        if (!statusCode.isError()) {
            log.warn("Desired status for error was " + statusCode.value() + ", which is not error code. Replacing with 500");
            statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
        }

        String text = message == null ?
                statusCode.reasonPhrase() : message;

        return new HttpResponse<>(HttpHeaders.NO_HEADERS, new ApiError(statusCode.value(), text, path), statusCode);
    }

    private Map<String, Map<HttpMethod, SingleEndpoint<?>>> createMappings() {
        final Map<String, Map<HttpMethod, SingleEndpoint<?>>> mappings = new HashMap<>();

        for (Method method : parentInstance.getClass().getDeclaredMethods()) {
            if (method.getDeclaringClass() == Object.class) {
                continue;
            }

            final RequestMapping mapping = method.getDeclaredAnnotation(RequestMapping.class);

            if (mapping == null) {
                continue;
            }

            String path = mapping.value();
            if (!CommonUtils.isEmpty(path)) {
                path = URLQueryUtils.stripLeadingSlashes(path);
                path = URLQueryUtils.stripEndingSlashes(path);
            } else {
                path = null; // both null and empty string will be treated same, so there won't be any mysterious bug
            }

            if (mappings.containsKey(path) && mappings.get(path).containsKey(mapping.method())) {
                throw new IllegalStateException("Path '" + path + "' for parent '" + parentPath + "' with method '" + mapping.method() + "' is a duplicate!");
            }

            method.setAccessible(true);
            var endpoint = new SingleEndpoint<>(parentInstance, path, mapping.method(), method);

            if (!mappings.containsKey(path)) {
                Map<HttpMethod, SingleEndpoint<?>> m = new HashMap<>();
                m.put(endpoint.httpMethod(), endpoint);
                mappings.put(path, m);
            } else {
                mappings.get(path).put(endpoint.httpMethod(), endpoint);
            }
        }

        return Collections.unmodifiableMap(mappings);
    }

    public record SingleEndpoint<T> (
        T parentClassInstance,
        String path,
        HttpMethod httpMethod,
        Method method
    ) {}

    public Map<String, Map<HttpMethod, SingleEndpoint<?>>> getEndpoints() {
        return endpoints; // endpoints are already unmodifiable view
    }
}
