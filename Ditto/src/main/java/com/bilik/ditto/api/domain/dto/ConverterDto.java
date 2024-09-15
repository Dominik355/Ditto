package com.bilik.ditto.api.domain.dto;

import java.util.ArrayList;
import java.util.Collection;

public class ConverterDto {

    private String name;
    private Collection<String> specificType;
    private Collection<String> args;

    public ConverterDto(String name, Collection<String> specificType, Collection<String> args) {
        this.name = name;
        this.specificType = specificType;
        this.args = args;
    }

    public String getName() {
        return name;
    }

    public Collection<String> getSpecificType() {
        return specificType;
    }

    public Collection<String> getArgs() {
        return args;
    }

    public void addArgs(Collection<String> args) {
        if (this.args == null) {
            this.args = new ArrayList<>();
        }
        this.args.addAll(args);
    }
}
