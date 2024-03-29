package io.streamnative.kstream.json;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.streamnative.model.DataRecord;
import io.streamnative.util.PropertyLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsExample {

    static final String topic = "persistent://public/default/json-data";

    public static void main(String[] args) throws Exception {

        final Properties props = PropertyLoader.loadConfig(args[0]);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-streams-1");
        // Disable caching to print the aggregation value after each record
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<DataRecord> DataRecord = getJsonSerde();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, DataRecord> records = builder.stream(topic, Consumed.with(Serdes.String(), DataRecord));

        KStream<String,Long> counts = records.map((k, v) -> new KeyValue<String, Long>(k, v.getCount()));
        counts.print(Printed.<String,Long>toSysOut().withLabel("Consumed record"));

        // Aggregate values by key
        KStream<String,Long> countAgg = counts.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(
                        (aggValue, newValue) -> aggValue + newValue)
                .toStream();
        countAgg.print(Printed.<String,Long>toSysOut().withLabel("Running count"));

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

    private static Serde<DataRecord> getJsonSerde(){

        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("json.value.type", DataRecord.class);

        final Serializer<DataRecord> mySerializer = new KafkaJsonSerializer<>();
        mySerializer.configure(serdeProps, false);

        final Deserializer<DataRecord> myDeserializer = new KafkaJsonDeserializer<>();
        myDeserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(mySerializer, myDeserializer);
    }
}
