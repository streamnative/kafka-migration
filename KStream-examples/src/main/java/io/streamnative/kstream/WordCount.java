package io.streamnative.kstream;

import io.streamnative.util.PropertyLoader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;


import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {

    public static void main(final String[] args) throws Exception {

        final Properties props = PropertyLoader.loadConfig(args[0]);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksConfigSetter.class);

        final StreamsBuilder builder = new StreamsBuilder();

        // KStream<String, String> source = builder.stream("persistent://public/default/sn-test-topic3");
        KStream<String, String> source = builder.stream("feed-flow-info");
        // KStream<String, String> source = builder.stream("feed-flow-info");
        source//.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                // .groupBy((key, value) -> value)
                .groupBy((key, value) -> key)
                // .count(Materialized.as(Stores.persistentKeyValueStore("counts-rocks-store")))
                .count(Materialized.as(Stores.inMemoryKeyValueStore("counts-store")))
                .toStream()
                .peek((k, v) -> { System.out.println(String.format("Key : %s   Value: %s", k,v)); })
                // .to("persistent://public/default/sn-test-output-topic",
                .to("sn-test-output-topic2",
                        Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (streams.state() != KafkaStreams.State.RUNNING) {
                    try {
                        Thread.sleep(1000);
                        System.out.println("Current stream status: " + streams.state());
                    } catch (InterruptedException e) {
                        System.out.println("The KStream application is interrupted: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                }
            }
        }).start();

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
