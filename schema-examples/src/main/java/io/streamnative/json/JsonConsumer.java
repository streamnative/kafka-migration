package io.streamnative.json;

import io.streamnative.model.DataRecord;
import io.streamnative.util.PropertyLoader;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class JsonConsumer {

    static final String topic = "persistent://public/default/json-data";

    public static void main(String[] args) throws IOException {

        final Properties props = PropertyLoader.loadConfig(args[0]);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, DataRecord.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, DataRecord> consumer = new KafkaConsumer<String, DataRecord>(props);
        consumer.subscribe(Collections.singleton((topic)));

        Long total_count = 0L;

        try {
            while (true) {
                ConsumerRecords<String, DataRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, DataRecord> record : records) {
                    String key = record.key();
                    DataRecord value = record.value();
                    total_count += value.getCount();
                    System.out.printf("Consumed record with key %s and value %s, and updated total count to %d%n", key, value, total_count);
                }
            }
        } finally {
            consumer.close();
        }
    }
}
