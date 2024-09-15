package com.bilik.ditto.gateway.dto;

import java.util.AbstractList;
import java.util.List;

public class Listing extends AbstractList<String> {

    private final List<String> list;

    public Listing(List<String> list) {
        this.list = list;
    }

    public static Listing from(List<String> list) {
        return new Listing(list);
    }

    public List<String> getList() {
        return list;
    }

    @Override
    public String get(int index) {
        return list.get(index);
    }

    @Override
    public int size() {
        return list.size();
    }
}
