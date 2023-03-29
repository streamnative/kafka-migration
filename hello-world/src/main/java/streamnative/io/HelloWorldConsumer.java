package streamnative.io;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import io.streamnative.util.PropertyLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class HelloWorldConsumer {

    public static void main(String[] args) throws IOException {

        final Properties props = PropertyLoader.loadConfig(args[0]);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hello-world");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        @SuppressWarnings("resource")
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("persistent://public/default/hello"));

        // 2. Consume some messages and quit immediately
        boolean running = true;
        while (running) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(600));
            if (!records.isEmpty()) {
                records.forEach(record -> System.out.println("Receive record: " + record.value() + " from "
                        + record.topic() + "-" + record.partition() + "@" + record.offset()));

            } else {
                running = false;
            }
        }
        System.out.println("Stopping consumer");
        consumer.close();
    }
}
