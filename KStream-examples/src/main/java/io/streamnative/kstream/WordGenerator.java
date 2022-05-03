package io.streamnative.kstream;

import io.streamnative.util.PropertyLoader;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class WordGenerator {

    private static final String[] SENTENCES = new String[]
            {"The quick brown fox jumped over the lazy dog",
            "It was the best of times, it was the worst of times",
            "A long time ago in a galaxy far, far away",
            "Who is afraid of the big bad wolf"};

    private static final Random rnd = new Random();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        final Properties props = PropertyLoader.loadConfig(args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int idx = 0; idx < 100; idx++) {
            String word = SENTENCES[rnd.nextInt(SENTENCES.length)];
            final Future<RecordMetadata> recordMetadataFuture = producer.send(
                    new ProducerRecord<>("persistent://public/default/sentence-topic", word));

            final RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println("Send "  + word + " to " + recordMetadata);
        }

    }
}
