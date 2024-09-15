package com.bilik.ditto.core.common;

import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DurationResolver {
    // DURATION
    static Map<Pattern, Function<Long, Duration>> DURATION_RESOLVER_MAP;

    static {
        DURATION_RESOLVER_MAP = Map.of(
                Pattern.compile("(\\d+)\\s*d(ays?)?"), Duration::ofDays,
                Pattern.compile("(\\d+)\\s*h(ours?)?"), Duration::ofHours,
                Pattern.compile("(\\d+)\\s*m(in(utes?)?)?"), Duration::ofMinutes,
                Pattern.compile("(\\d+)\\s*s(ec(onds?)?)?"), Duration::ofSeconds,
                Pattern.compile("(\\d+)\\s*m(ill?i)?s(ec(onds?)?)?"), Duration::ofMillis,
                Pattern.compile("(\\d+)\\s*(u|micro)s(ec(onds?)?)?"), micro -> Duration.of(micro, ChronoUnit.MICROS),
                Pattern.compile("(\\d+)\\s*n(ano)?s(ec(onds?)?)?"), Duration::ofNanos
        );
    }

    public static Duration resolveDuration(String input) {
        if (StringUtils.isEmpty(input)) {
            return null;
        }
        Matcher matcher;
        for (Map.Entry<Pattern, Function<Long, Duration>> entry : DURATION_RESOLVER_MAP.entrySet()) {
            if ((matcher = entry.getKey().matcher(input)).matches()) {
                return entry.getValue().apply(Long.valueOf(matcher.group(1)));
            }
        }
        throw new RuntimeException("Duration for input [" + input + "] couldn't be resolved");
    }

}
