package com.bilik.ditto.api.domain.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class ConnectivityDto {

    private Map<String, List<ConnectivityInfo>> info;

    public ConnectivityDto(Map<String, Map<String, String>> info) {
        this.info = info.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().entrySet().stream()
                                .map(innerEntry -> new ConnectivityInfo(innerEntry.getKey(), innerEntry.getValue()))
                                .toList()
                ));

    }

    @Data
    public static class ConnectivityInfo {

        public ConnectivityInfo(String name, String exception) {
            this.name = name;
            this.available = exception == null;
            this.exception = exception;
        }

        String name;
        boolean available;
        String exception;
    }

}
