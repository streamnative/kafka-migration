package io.streamnative.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.streamnative.util.PropertyLoader;
import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvroConsumer {

    static final String topic = "persistent://public/default/avro-data";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        final Properties props = PropertyLoader.loadConfig(args[0]);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-avro-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, DataRecordAvro> consumer = new KafkaConsumer<String, DataRecordAvro>(props);
        consumer.subscribe(Arrays.asList(topic));

        Long total_count = 0L;

        try {
            while (true) {
                ConsumerRecords<String, DataRecordAvro> records = consumer.poll(100);
                for (ConsumerRecord<String, DataRecordAvro> record : records) {
                    String key = record.key();
                    DataRecordAvro value = record.value();
                    total_count += value.getCount();
                    System.out.printf("Consumed record with key %s and value %s, and updated total count to %d%n", key, value, total_count);
                }
            }
        } finally {
            consumer.close();
        }
    }

}
