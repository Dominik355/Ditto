package com.bilik.ditto.gateway.server.objects;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class HttpHeaders implements Map<String,List<String>>{

    // put here any header constant which u may use
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_LENGTH = "Content-Length";

    public static final HttpHeaders NO_HEADERS = new HttpHeaders(Collections.emptyMap());

    private final Map<String, List<String>> map;

    public HttpHeaders(Map<String, List<String>> headers) {
        this.map = new HashMap<>(headers);
    }

    public Map<String, List<String>> getHeaders() {
        return Collections.unmodifiableMap(map);
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return key != null && map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public List<String> get(Object key) {
        return map.get(key);
    }

    public String getFirst(String key) {
        var list = map.get(key);
        return list == null ? null : list.get(0);
    }

    public List<String> put(String key, List<String> value) {
        return map.put(key, value);
    }

    @Override
    public List<String> remove(Object key) {
        return map.remove(key);
    }

    public void set(String key, String value) {
        var list = map.getOrDefault(key, new LinkedList<>());
        list.add(value);
        put(key, list);
    }

    public void putAll(Map<? extends String,? extends List<String>> t)  {
        map.putAll(t);
    }

    public void clear() {
        map.clear();
    }

    public Set<String> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<List<String>> values() {
        return map.values();
    }

    @Override
    public Set<Entry<String, List<String>>> entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return map.equals(o);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", HttpHeaders.class.getSimpleName() + "[", "]")
                .add("map=" + map)
                .toString();
    }
}
