package me.aki.flink.examples.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;

/**
 * @author akis
 * @date 2021/1/20
 **/
public class FlinkKafkaProducerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test-1");

        // 这个方法会导致每个 subtask 的数据都写入到同一个 partition，默认 at_least_once
        FlinkKafkaProducer<String> myProducer1 = new FlinkKafkaProducer<>(
                "my-topic1",          // target topic
                new SimpleStringSchema(),    // serialization schema
                properties);                 // producer config


        // 默认 at_least_once
        FlinkKafkaProducer<String> myProducer2 = new FlinkKafkaProducer<>(
                "my-topic1",          // target topic
                new SimpleStringSchema(),    // serialization schema
                properties,                  // producer config
                Optional.empty());           // partitioner

        // 完整参数的构造方法
        FlinkKafkaProducer<String> myProducer3 = new FlinkKafkaProducer<>(
                "my-topic1",          // target topic
                new SimpleStringSchema(),    // serialization schema
                properties,                  // producer config
                null,         // partitioner
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
                FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE
        );

        // deprecated，并且这个方法会导致每个 subtask 的数据都写入到同一个 partition
        // 也就是只有一个并行度的时候，所有的数据都会写到一个 paritition
        FlinkKafkaProducer<String> myProducer4 = new FlinkKafkaProducer<>(
                "my-topic1",                  // target topic
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),    // serialization schema
                properties,                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance


        // deprecated，解决了所有数据写到同一个 partition，会采用轮询的方式均匀写入到 partition
        FlinkKafkaProducer<String> myProducer5 = new FlinkKafkaProducer<>(
                "my-topic1",                  // target topic
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),    // serialization schema
                properties,                  // producer config
                Optional.empty()); // partitioner


        // 需要自己实现 KafkaSerializationSchema
        FlinkKafkaProducer<String> myProducer6 =
                new FlinkKafkaProducer<>(
                        "my-topic1",
                        new ProducerStringSerializationSchema("my-topic1"),
                        properties,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
                );


        env.fromElements("1", "2", "3", "4", "5")
                .addSink(myProducer6);


        env.execute();
    }

    public static class ProducerStringSerializationSchema implements KafkaSerializationSchema<String> {

        private final String topic;

        public ProducerStringSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
            return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
        }

    }
}
