package com.bilik.ditto.gateway.endpoints;

import com.bilik.ditto.gateway.DittoService;
import com.bilik.ditto.gateway.server.RequestMapping;
import com.bilik.ditto.gateway.server.objects.HttpHeaders;
import com.bilik.ditto.gateway.server.objects.HttpMethod;
import com.bilik.ditto.gateway.server.objects.HttpResponse;
import com.bilik.ditto.gateway.server.objects.HttpStatus;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class WelcomeEndpoint {

    private final DittoService service;

    public WelcomeEndpoint(DittoService service) {
        this.service = service;
    }

    @RequestMapping(method = HttpMethod.GET)
    public HttpResponse<String> welcome() {
        return new HttpResponse<>(
                new HttpHeaders(Map.of(
                        "Content-Type", List.of("text/html; charset=utf-8"),
                        "Accept-Ranges", List.of("bytes")
                )),
                new String(getHtml(), StandardCharsets.UTF_8).replaceAll("IP_PLACEHOLDER", service.getNextIP()),
                HttpStatus.OK
        );
    }

    private byte[] getHtml() {
        try {
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream is = classloader.getResourceAsStream("welcomePage.html");
            return is.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException("welcomePage.html file not found !", e);
        }
    }

}
