package me.aki.flink.examples.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * author akis
 * Date 2020/12/22
 */
public class TimeUtil {

    public static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final String DEFAULT_ZONE_OFFSET = "+08:00";

    public static long parse(String time) {
        LocalDateTime localDateTime = LocalDateTime.parse(time, DEFAULT_FORMATTER);
        return localDateTime.toInstant(ZoneOffset.of(DEFAULT_ZONE_OFFSET)).toEpochMilli();
    }

    public static String format(long millis) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(DEFAULT_ZONE_OFFSET));
        return DEFAULT_FORMATTER.format(localDateTime);
    }
}
