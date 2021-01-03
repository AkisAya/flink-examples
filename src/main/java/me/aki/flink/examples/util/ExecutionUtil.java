package me.aki.flink.examples.util;

import me.aki.flink.examples.kafka.MyFlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author akis
 * @date 2020/6/10
 **/
public class ExecutionUtil {
    public static ParameterTool fromArgsAndProps(String[] args, String propsFile) throws IOException {
        InputStream in = ExecutionUtil.class.getResourceAsStream(propsFile);
        return ParameterTool.fromPropertiesFile(in)
                .mergeWith(ParameterTool.fromArgs(args));
    }


    /**
     * suppose input kafka value is: key, value, time, example: a, 1, 2021-01-04 12:00:00
     * @param env
     * @param properties
     * @param topic
     * @param checkpointEnabled
     * @param idleness
     * @return
     */
    public static DataStreamSource<String> customKafkaStream(StreamExecutionEnvironment env, Properties properties,
                                                             String topic, boolean checkpointEnabled, long idleness) {
        MyFlinkKafkaConsumer<String> consumer = new MyFlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties);

        //指定消费kafka的方式
        consumer.setCommitOffsetsOnCheckpoints(checkpointEnabled);
        consumer.setIdleTimeout(idleness);
        consumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String time = element.split(",\\s*")[2];
                return TimeUtil.parse(time);
            }
        });
        return env.addSource(consumer);
    }

    public static DataStreamSource<String> kafkaStream(StreamExecutionEnvironment env, Properties properties,
                                                             String topic, boolean checkpointEnabled) {
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties);

        //指定消费kafka的方式
        consumer.setCommitOffsetsOnCheckpoints(checkpointEnabled);
        consumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String time = element.split(",\\s*")[2];
                return TimeUtil.parse(time);
            }
        });
        return env.addSource(consumer);
    }
}

