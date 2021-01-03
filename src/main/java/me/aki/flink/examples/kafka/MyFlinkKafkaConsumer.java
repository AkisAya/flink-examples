package me.aki.flink.examples.kafka;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author akis
 * @date 2021/1/4
 */
@Slf4j
public class MyFlinkKafkaConsumer<T> extends FlinkKafkaConsumer<T> {
    @Setter
    private long idleTimeout;

    public MyFlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
    }

    public MyFlinkKafkaConsumer(String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(topic, deserializer, props);
    }

    public MyFlinkKafkaConsumer(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
        super(topics, deserializer, props);
    }

    public MyFlinkKafkaConsumer(List<String> topics, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(topics, deserializer, props);
    }

    public MyFlinkKafkaConsumer(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, Properties props) {
        super(subscriptionPattern, valueDeserializer, props);
    }

    public MyFlinkKafkaConsumer(Pattern subscriptionPattern, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(subscriptionPattern, deserializer, props);
    }



    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        Class<?> clz = sourceContext.getClass().getSuperclass();
        Field idleTimeoutField = clz.getDeclaredField("idleTimeout");
        idleTimeoutField.setAccessible(true);
        idleTimeoutField.set(sourceContext, idleTimeout);
        super.run(sourceContext);
    }
}
