package me.aki.flink.examples.watermark;

import me.aki.flink.examples.util.TimeUtil;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author akis
 * @date 2020/12/24
 */
public class EventWatermarkWithProcessTimeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 默认就是 200ms 会调用 onPeriodicEmit
        env.getConfig().setAutoWatermarkInterval(200);

        WatermarkStrategy<Tuple3<String, Integer, Long>> strategy = WatermarkStrategy
                .forGenerator(
                        (ctx) -> new WithProcessTimeWatermark(Duration.ofMinutes(1)))
                .withTimestampAssigner((event, timestamp) -> event.f2);

        env
            .socketTextStream("localhost", 9990)
            .map(new MyMapFunc())
            .assignTimestampsAndWatermarks(strategy)
            .keyBy(e -> e.f0)
            .window(TumblingEventTimeWindows.of(Time.minutes(2)))
            .max(1)
            .print();

        env.execute();
    }

    // input: key, 2, 2020-12-20 19:45:00
    private static class MyMapFunc extends RichMapFunction<String, Tuple3<String, Integer, Long>> {
        @Override
        public Tuple3<String, Integer, Long> map(String value) throws Exception {
            String[] res = value.split(",\\s*");
            return new Tuple3<>(res[0], Integer.valueOf(res[1]), TimeUtil.parse(res[2]));
        }
    }

    private static class WithProcessTimeWatermark implements WatermarkGenerator<Tuple3<String, Integer, Long>> {
        Logger log = LoggerFactory.getLogger(WithProcessTimeWatermark.class);
        /** The maximum timestamp encountered so far. */
        private long maxTimestamp;

        /** event time gap to current process time */
        private final long timeGap;

        /**
         * create a watermark, if currentProcessTime - maxTimestamp > idleness, use currentProcessTime as maxTimestamp
         * @param timeGap
         */
        public WithProcessTimeWatermark(Duration timeGap) {
            checkNotNull(timeGap, "idleness");
            checkArgument(!timeGap.isNegative(), "idleness cannot be negative");

            this.timeGap = timeGap.toMillis();

            // start so that our lowest watermark would be Long.MIN_VALUE.
            this.maxTimestamp = Long.MIN_VALUE + 1;
        }

        @Override
        public void onEvent(Tuple3<String, Integer, Long> event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            log.debug("onPeriodicEmit called");
            long currentProcessTime = System.currentTimeMillis();
            if (currentProcessTime - maxTimestamp > timeGap) {
                log.info("current processing time is {}, current max timestamp is {}, " +
                        "maxTimestamp has not been updated by event for {}ms, update maxTimestamp to current processing time",
                        currentProcessTime, maxTimestamp, timeGap);
                maxTimestamp = currentProcessTime;
            }
            output.emitWatermark(new Watermark(maxTimestamp - 1));
        }
    }


}
