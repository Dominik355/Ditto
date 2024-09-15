package com.bilik.ditto.core.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Very ... very simple replacement for Metrics
 */
public class CounterAggregator {

    private static final Logger log = LoggerFactory.getLogger(CounterAggregator.class);

    private final Map<String, Counter> counters = new HashMap<>();

    public Counter addCounter(String name, Counter counter) {
        if (counter == null) {
            log.warn("Ignoring attempted registration of a counter of name {}.", name);
        }

        synchronized (this) {
            if (counters.containsKey(name)) {
                log.warn("Metric was not added because of name collision [name = {}]", name);
            } else {
                counters.put(name, counter);
            }
        }

        return counter;
    }

    public Counter getCounter(String name) {
        return counters.get(name);
    }

    public Map<String, Counter> getCounters() {
        return counters;
    }

    public Map<String, Counter> viewCounters() {
        return Collections.unmodifiableMap(counters);
    }

    @Override
    public String toString() {
        return counters.entrySet().stream()
                .map(entry -> entry.getKey() + " | " + entry.getValue().getCount())
                .reduce((str1, str2) -> str1 + "\n" + str2)
                .orElse("EMPTY COUNTERS");
    }
}
