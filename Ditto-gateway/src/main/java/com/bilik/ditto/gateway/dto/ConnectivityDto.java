package com.bilik.ditto.gateway.dto;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class ConnectivityDto {

    private Map<String, List<ConnectivityInfo>> info;

    public ConnectivityDto() {}

    public ConnectivityDto(Map<String, Map<String, String>> info) {
        this.info = info.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().entrySet().stream()
                                .map(innerEntry -> new ConnectivityInfo(innerEntry.getKey(), innerEntry.getValue()))
                                .toList()
                ));

    }

    public Map<String, List<ConnectivityInfo>> getInfo() {
        return info;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ConnectivityDto.class.getSimpleName() + "[", "]")
                .add("info=" + info)
                .toString();
    }

    public static class ConnectivityInfo {

        private String name;
        private boolean available;
        private String exception;

        public ConnectivityInfo() {}

        public ConnectivityInfo(String name, String exception) {
            this.name = name;
            this.available = exception == null;
            this.exception = exception;
        }

        public String getName() {
            return name;
        }

        public boolean isAvailable() {
            return available;
        }

        public String getException() {
            return exception;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", ConnectivityInfo.class.getSimpleName() + "[", "]")
                    .add("name='" + name + "'")
                    .add("available=" + available)
                    .add("exception='" + exception + "'")
                    .toString();
        }

    }
}
