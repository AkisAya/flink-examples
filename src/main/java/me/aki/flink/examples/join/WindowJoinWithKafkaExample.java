package me.aki.flink.examples.join;

import me.aki.flink.examples.util.ExecutionUtil;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Properties;

/**
 * @author akis
 * @date 2021/1/4
 */
public class WindowJoinWithKafkaExample {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool =  ExecutionUtil.fromArgsAndProps(args, "/app.properties");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(200);
        env.setParallelism(1);

        String kafkaServers = "172.27.133.28:9092";
        String leftTopic = "left";
        String rightTopic = "right";

        Properties leftProps = new Properties();
        leftProps.setProperty("bootstrap.servers", kafkaServers);
        leftProps.setProperty("group.id", "left");

        Properties rightProps = new Properties();
        rightProps.setProperty("bootstrap.servers", kafkaServers);
        rightProps.setProperty("group.id", "right");


        long idleness = Time.seconds(10).toMilliseconds();

        boolean useCustomKafkaConsumer = parameterTool.getBoolean("custom.kafka.consumer", false);

        DataStream<Tuple2<String, Integer>> leftStream;
        DataStream<Tuple2<String, Integer>> rightStream;
        if (useCustomKafkaConsumer) {
            leftStream = ExecutionUtil.customKafkaStream(env, leftProps, leftTopic, false, idleness)
                    .map(new MyMapFunction());
            rightStream = ExecutionUtil.customKafkaStream(env, rightProps, rightTopic, false, idleness)
                    .map(new MyMapFunction());
        } else {
            leftStream = ExecutionUtil.kafkaStream(env, leftProps, leftTopic, false)
                    .map(new MyMapFunction());
            rightStream = ExecutionUtil.kafkaStream(env, rightProps, rightTopic, false)
                    .map(new MyMapFunction());
        }

        leftStream.join(rightStream)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>(){
                    @Override
                    public Tuple2<String, Integer> join(Tuple2<String, Integer> left, Tuple2<String, Integer> right) {
                        return new Tuple2<>(left.f0, left.f1 + right.f1);
                    }
                })
                .print();


        env.execute();
    }

    /**
     * input kafka string is: key, value, time, example: a, 1, 2021-01-04 12:00:00
     * output tuple2: (key, value)
     */
    static class MyMapFunction extends RichMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            String[] arr = value.split(",\\s*");
            return new Tuple2<>(arr[0], Integer.valueOf(arr[1]));
        }
    }
}
