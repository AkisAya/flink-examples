package me.aki.flink.examples.window;

import me.aki.flink.examples.util.TimeUtil;

/**
 * @author akis
 * @date 2021/1/6
 */
public class Main {
    public static void main(String[] args) {
        long timestamp = TimeUtil.parse("2020-12-20 12:00:00");
        long offset = - 3 * 3600 * 1000L;
        long windowSize = 24 * 3600 * 1000L;

        System.out.println(TimeUtil.format(getWindowStartWithOffset(timestamp, offset, windowSize)));
    }

    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        return timestamp - (timestamp - offset + windowSize) % windowSize;
    }
}
