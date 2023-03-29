package io.streamnative.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
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

        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "https://gcp-cent1-cb4bd17b-a875-4cd1-a83c-948cdfed90a4.gcp-shared-gcp-usce1-martin.streamnative.g.snio.cloud/");

        props.put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        props.put(KafkaAvroSerializerConfig.USER_INFO_CONFIG, String.format("%s:%s", "public",
                "token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik5rRXdSVVU1TUVOQlJrWTJNalEzTVRZek9FVkZRVVUyT0RNME5qUkRRVEU1T1VNMU16STVPUSJ9.eyJodHRwczovL3N0cmVhbW5hdGl2ZS5pby91c2VybmFtZSI6InN1cGVyQG8tN25nN3YuYXV0aC5zdHJlYW1uYXRpdmUuY2xvdWQiLCJpc3MiOiJodHRwczovL2F1dGguc3RyZWFtbmF0aXZlLmNsb3VkLyIsInN1YiI6ImkxWG9CYjY4RVA5UFRrWEpSOU93akFTckJoN1BGR0NIQGNsaWVudHMiLCJhdWQiOiJ1cm46c246cHVsc2FyOm8tN25nN3Y6c21jLWtvcCIsImlhdCI6MTY3OTM1NDU2OSwiZXhwIjoxNjc5OTU5MzY5LCJhenAiOiJpMVhvQmI2OEVQOVBUa1hKUjlPd2pBU3JCaDdQRkdDSCIsInNjb3BlIjoiYWRtaW4gYWNjZXNzIiwiZ3R5IjoiY2xpZW50LWNyZWRlbnRpYWxzIiwicGVybWlzc2lvbnMiOlsiYWRtaW4iLCJhY2Nlc3MiXX0.c2hE2rsdzrOjlWPp-L2IPh4iyUWRmOZ5lal6ccflqlvhOD2RLiq0kiC90TeSIEtrv1WAL9_rUGNxdRcvWsU-KjsYGsmayyq1hKRfM13WEHXUnA4iJFwFSWYC1-Mwr346fc3c2W57usZtjPCuJkb7mYoulwT-F8zXUP8tTdKBkRKW6l5EVcz8erU99mfArBgLkQ94L4zaXeQkm9RUSK209EbIsqvOUY_PbdRC1yp4oOmOGQdIeAQFJ1ielKjACMasHzrMNxyI3Jjq11UVEgNOXpbum3rYE_fOJil04-nD0blNM4yC6qHYdZEEN9JQNopwj6D8XdPIcKJS6JPBOJeCxg"));

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
