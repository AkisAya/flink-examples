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

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
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
        LocalDateTime localDateTime = LocalDateTime.parse(time, DATE_TIME_FORMATTER);
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
        return DATE_TIME_FORMATTER.format(localDateTime);
    }


    public static final long MS_15MIN = 15 * 60 * 1000;
    public static final long MS_30MIN = 30 * 60 * 1000;
    public static final long MS_8HOUR = 8 * 60 * 60 * 1000;
    public static final long MS_1DAY = 24 * 60 * 60 * 1000;

    /**
     *
     * @param ts           当前时间戳
     * @param windowSize   时窗大小
     * @param offset       时区偏移
     * @return
     */
    static private long align(long ts, long windowSize, long offset) {
        // return ((ts + offset) - (ts + offset) % windowSize) - offset;
        // simplified as below
        return ts - (ts + offset) % windowSize;
    }

    public static long alignTo15MinMs(long ts) {
        return align(ts, MS_15MIN, 0);
    }

    public static long alignTo30MinMs(long ts) {
        return align(ts, MS_30MIN, 0);
    }

    public static long alignTo1DayMs(long ts) {
        return align(ts, MS_1DAY, MS_8HOUR);
    }
}
