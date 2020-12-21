package me.aki.flink.examples;

import lombok.extern.slf4j.Slf4j;
import me.aki.flink.examples.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * author akis
 * Date 2020/12/21
 *
 * nc -l 9990  a,left1,2020-12-20 12:12:30
 * nc -l 9991  a,right1,2020-12-20 12:12:45
 */
@Slf4j
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        log.info("start....");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // flink 1.12 开始，默认使用 event time
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        WatermarkStrategy<Tuple3<String, String, Long>> strategy = WatermarkStrategy
                .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.f2);

        DataStream<Tuple3<String, String, Long>> leftStream = env
                .socketTextStream("localhost", 9990)
                .map(new MyMapFunc())
                .assignTimestampsAndWatermarks(strategy);

        DataStream<Tuple3<String, String, Long>> rightStream = env
                .socketTextStream("localhost", 9991)
                .map(new MyMapFunc())
                .assignTimestampsAndWatermarks(strategy);


        leftStream.keyBy(e -> e.f0)
                .intervalJoin(rightStream.keyBy(e -> e.f0))
                .between(Time.minutes(0), Time.minutes(1))
                .process(new MyProcessFunction())
                .print();


        env.execute();
    }

    /**
     * input: key, value, 2020-12-20 19:45:00
     */
    private static class MyMapFunc extends RichMapFunction<String, Tuple3<String, String, Long>> {
        @Override
        public Tuple3<String, String, Long> map(String value) throws Exception {
            String[] res = value.split(",");
            return new Tuple3<>(res[0], res[1], TimeUtil.parse(res[2]));
        }
    }

    private static class MyProcessFunction extends ProcessJoinFunction<
            Tuple3<String, String, Long>,
            Tuple3<String, String, Long>,
            Tuple5<String, String, String, String, String>> {
        @Override
        public void processElement(Tuple3<String, String, Long> left,
                                   Tuple3<String, String, Long> right,
                                   Context ctx,
                                   Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
            log.info("joined: {}, {}", left, right);
            out.collect(new Tuple5<>(left.f0, left.f1, TimeUtil.format(left.f2), right.f1, TimeUtil.format(right.f2)));
        }
    }
}
