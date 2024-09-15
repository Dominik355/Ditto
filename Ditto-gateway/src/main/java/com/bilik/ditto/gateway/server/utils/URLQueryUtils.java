package com.bilik.ditto.gateway.server.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.bilik.ditto.gateway.server.utils.CommonUtils.isEmpty;

public class URLQueryUtils {

    private static final Logger log = LoggerFactory.getLogger(URLQueryUtils.class);

    /**
     * LinkedHashMap used, so we keep order or parameters
     * Custom collector, because toMap() does not allow null values
     */
    public static Map<String, String> splitQuery(URI uri) {
        if (isEmpty(uri.getQuery())) {
            return Collections.emptyMap();
        }
        return Arrays.stream(uri.getQuery().split("&"))
                .map(URLQueryUtils::splitQueryParameter)
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
    }

    public static Map.Entry<String, String> splitQueryParameter(String it) {
        final int idx = it.indexOf("=");
        final String key = idx > 0 ? it.substring(0, idx) : it;
        final String value = idx > 0 && it.length() > idx + 1 ? it.substring(idx + 1) : null;
        return new AbstractMap.SimpleImmutableEntry<>(
                decodeUTF8(key),
                decodeUTF8(value)
        );
    }

    public static String decodeUTF8(String val) {
        return val == null ? null : URLDecoder.decode(val, StandardCharsets.UTF_8);
    }

    public static String stripLeadingSlashes(String path) {
        if (path.startsWith("/")) {
            String stripped = StringUtils.stripStart(path, "/");
            log.info("Stripping starting slashes from path '" + path + "'. Registering path '" + path + " instead");
            path = stripped;
        }
        return path;
    }

    public static String stripEndingSlashes(String path) {
        if (path.endsWith("/")) {
            String stripped = StringUtils.stripEnd(path, "/");
            log.info("Stripping ending slashes from path '" + path + "'. Registering path '" + path + " instead");
            path = stripped;
        }
        return path;
    }

}
