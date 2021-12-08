package me.aki.flink.examples.window;

import me.aki.flink.examples.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @author akis
 * @date 2021/1/6
 */
public class WindowWithOffset {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 默认就是 200ms 会调用 onPeriodicEmit
        env.getConfig().setAutoWatermarkInterval(200);

        WatermarkStrategy<Tuple3<String, Integer, Long>> strategy = WatermarkStrategy
                .<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.f2);

        env
                .socketTextStream("localhost", 9999)
                .map(new MyMapFunc())
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-3)))
                .process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<String> out) throws Exception {
                        String windowStart = TimeUtil.format(context.window().getStart());
                        String windowEnd = TimeUtil.format(context.window().getEnd());
                        System.out.printf("window start: %s, window end: %s%n", windowStart, windowEnd);
                        for (Tuple3<String, Integer, Long> ele : elements) {
                            out.collect(String.valueOf(ele.f1));
                        }
                    }
                }).print();

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
}
