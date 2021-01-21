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

    public static final DateTimeFormatter defaultFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final String DEFAULT_ZONE_OFFSET = "+08:00";

    public static long parse(String time) {
        return parse(time, DEFAULT_ZONE_OFFSET);
    }

    public static String format(long millis) {
        return format(millis, DEFAULT_ZONE_OFFSET);
    }

    /**
     *
     * @param time formatted time
     * @param timeZoneOffset 时区偏移, 如： +08:00
     * @return
     */
    public static long parse(String time, String timeZoneOffset) {
        LocalDateTime localDateTime = LocalDateTime.parse(time, defaultFormatter);
        return localDateTime.toInstant(ZoneOffset.of(timeZoneOffset)).toEpochMilli();
    }

    /**
     *
     * @param millis timestamp millis
     * @param timeZoneOffset 时区偏移, 如： +08:00
     * @return
     */
    public static String format(long millis, String timeZoneOffset) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(timeZoneOffset));
        return defaultFormatter.format(localDateTime);
    }
}
