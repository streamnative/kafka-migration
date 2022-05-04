package io.streamnative.kstream.processor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountProcessorDemo {
    static class WordCountProcessor implements Processor<String, String, String, String> {
        private KeyValueStore<String, Long> kvStore;

        @Override
        public void init(final ProcessorContext<String, String> context) {
            context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                try (final KeyValueIterator<String, Long> iter = kvStore.all()) {
                    System.out.println("----------- " + timestamp + " ----------- ");

                    while (iter.hasNext()) {
                        final KeyValue<String, Long> entry = iter.next();

                        System.out.println("[" + entry.key + ", " + entry.value + "]");

                        context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
                    }
                }
            });
            kvStore = context.getStateStore("Counts");
        }

        @Override
        public void process(final Record<String, String> record) {
            final String[] words = record.value().toLowerCase(Locale.getDefault()).split("\\W+");

            for (final String word : words) {
                final Long oldValue = kvStore.get(word);

                if (oldValue == null) {
                    kvStore.put(word, 1L);
                } else {
                    kvStore.put(word, oldValue + 1);
                }
            }
        }

        @Override
        public void close() {
            // close any resources managed by this processor
            // Note: Do not close any StateStores as these are managed by the library
        }
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }

        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Topology builder = new Topology();

        builder.addSource("Source", "persistent://public/default/sentence-topic");

        builder.addProcessor("Process", WordCountProcessor::new, "Source");

        builder.addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("Counts"),
                        Serdes.String(),
                        Serdes.Long()),
                "Process");

        builder.addSink("Sink", "persistent://public/default/streams-word-count-output", "Process");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
