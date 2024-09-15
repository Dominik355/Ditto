package com.bilik.ditto.gateway.server.objects;

import com.bilik.ditto.gateway.server.utils.URLQueryUtils;

import java.net.URI;
import java.util.Map;
import java.util.StringJoiner;

public class HttpRequest {

    private final HttpMethod method;
    private final HttpHeaders headers;
    private final URI uri;
    private final String body;
    private final Map<String, String> params;

    public HttpRequest(HttpMethod method,
                       HttpHeaders headers,
                       URI uri,
                       String body) {
        this.method = method;
        this.headers = headers;
        this.uri = uri;
        this.body = body;
        this.params = URLQueryUtils.splitQuery(uri);
    }

    public HttpMethod getMethod() {
        return method;
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

    public URI getUri() {
        return uri;
    }

    public String getBody() {
        return body;
    }

    public String getPath() {
        return uri.getPath();
    }

    public Map<String, String> getParams() {
        return params;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", HttpRequest.class.getSimpleName() + "[", "]")
                .add("method=" + method)
                .add("headers=" + headers)
                .add("uri=" + uri)
                .add("body='" + body + "'")
                .add("params=" + params)
                .toString();
    }
}
