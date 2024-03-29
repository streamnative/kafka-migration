package io.streamnative.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.streamnative.util.PropertyLoader;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvroProducer {

    //   static final String topic = "persistent://public/default/avro-data";
    static final String topic = "persistent://tenant2/value-based-care/med-adh-in";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        final Properties props = PropertyLoader.loadConfig(args[0]);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "https://gcp-cent1-cb4bd17b-a875-4cd1-a83c-948cdfed90a4.gcp-shared-gcp-usce1-martin.streamnative.g.snio.cloud/");

        props.put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        props.put(KafkaAvroSerializerConfig.USER_INFO_CONFIG, String.format("%s:%s", "public",
                "token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik5rRXdSVVU1TUVOQlJrWTJNalEzTVRZek9FVkZRVVUyT0RNME5qUkRRVEU1T1VNMU16STVPUSJ9.eyJodHRwczovL3N0cmVhbW5hdGl2ZS5pby91c2VybmFtZSI6InN1cGVyQG8tN25nN3YuYXV0aC5zdHJlYW1uYXRpdmUuY2xvdWQiLCJpc3MiOiJodHRwczovL2F1dGguc3RyZWFtbmF0aXZlLmNsb3VkLyIsInN1YiI6ImkxWG9CYjY4RVA5UFRrWEpSOU93akFTckJoN1BGR0NIQGNsaWVudHMiLCJhdWQiOiJ1cm46c246cHVsc2FyOm8tN25nN3Y6c21jLWtvcCIsImlhdCI6MTY4MDAyNjE5MSwiZXhwIjoxNjgwNjMwOTkxLCJhenAiOiJpMVhvQmI2OEVQOVBUa1hKUjlPd2pBU3JCaDdQRkdDSCIsInNjb3BlIjoiYWRtaW4gYWNjZXNzIiwiZ3R5IjoiY2xpZW50LWNyZWRlbnRpYWxzIiwicGVybWlzc2lvbnMiOlsiYWRtaW4iLCJhY2Nlc3MiXX0.dyyxa31CJaoHxI7ISZEn7khgJ3Yp0rht_hBV8fJ4ZQekMcZ7kfl9PDvYNCG8yZvAoVweYTCKzHUa6QGEXHqGbjlZd7IPhpTbMQA43lr7QxFxaF2RGKMTiingwS-o0n3i_5TK_a8WiCT3JezpY6NJQEr1UPDrHp4jygAmU9ZPzVZDT_pIyLOXJ9pv5zyQrMDYaDKnHPmfcKMWi2zdPImlrtV4YGJGvkndTx104mhyPu7sXhlwl5pxTfrUS7LPhgkrIVOfs8QM7WHuoYB2UXQOjwlMN3uaGbZK6BTSpG04BMtka6p1VzGtiffqaUewkS0Eng_ky4RW6f3YurKGnM_vRw"));

        Producer<String, DataRecordAvro> producer = new KafkaProducer<String, DataRecordAvro>(props);

        // Produce sample data
        final Long numMessages = 10L;
        for (Long i = 0L; i < numMessages; i++) {
            String key = "alice";
            DataRecordAvro record = new DataRecordAvro(i);

            producer.send(new ProducerRecord<String, DataRecordAvro>(topic, key, record), new Callback() {
                @Override
                public void onCompletion(RecordMetadata m, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                    }
                }
            });
        }

        producer.flush();

        System.out.printf( numMessages + " messages were produced to topic %s%n", topic);

        producer.close();

    }
}
