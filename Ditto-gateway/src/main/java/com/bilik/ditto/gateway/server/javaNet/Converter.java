package com.bilik.ditto.gateway.server.javaNet;

import com.bilik.ditto.gateway.server.objects.HttpHeaders;
import com.bilik.ditto.gateway.server.objects.HttpMethod;
import com.bilik.ditto.gateway.server.objects.HttpRequest;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class Converter {

    public static HttpHeaders from(Headers headers) {
        return new HttpHeaders(headers);
    }

    public static void fillHeaders(Headers netHeaders, HttpHeaders intraHeaders) {
        netHeaders.putAll(intraHeaders);
    }

    public static HttpRequest from(HttpExchange s) {
        return new HttpRequest(
                HttpMethod.valueOf(s.getRequestMethod()),
                from(s.getRequestHeaders()),
                s.getRequestURI(),
                new BufferedReader(new InputStreamReader(s.getRequestBody()))
                        .lines().collect(Collectors.joining("\n"))
        );
    }

}
