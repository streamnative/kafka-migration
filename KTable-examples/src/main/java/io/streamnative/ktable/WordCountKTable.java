package io.streamnative.ktable;

import io.streamnative.util.PropertyLoader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountKTable {

    public static void main(final String[] args) throws Exception {

        final Properties props = PropertyLoader.loadConfig(args[0]);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream("persistent://public/default/sentence-topic");
        textLines.peek((k, v) -> { System.out.println(String.format("Processing %s", v)); });

        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count(Materialized.as(Stores.inMemoryKeyValueStore("counts-store")));

        wordCounts.toStream()
                .peek((k, v) -> { System.out.println("Key: " + k + " ,Value: " + v); })
                .to("persistent://public/default/streams-wordcount-output",
                Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
