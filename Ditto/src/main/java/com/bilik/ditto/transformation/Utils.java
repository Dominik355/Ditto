package com.bilik.ditto.transformation;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class Utils {

    public static OffsetDateTime getDateTime(long timestamp) {
        // because of microseconds we have to multiply 1000 so we can get nanosec
        LocalDateTime localDT = LocalDateTime.ofEpochSecond((timestamp/1000000), (int)(timestamp % 1000000) * 1000, ZoneOffset.UTC);
        return OffsetDateTime.of(localDT, ZoneOffset.UTC);
    }

}
