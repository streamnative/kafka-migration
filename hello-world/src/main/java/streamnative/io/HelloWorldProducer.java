package streamnative.io;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.streamnative.util.PropertyLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class HelloWorldProducer {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        final Properties props = PropertyLoader.loadConfig(args[0]);

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int idx = 0; idx < 1000; idx++) {
            final Future<RecordMetadata> recordMetadataFuture = producer.send(
                    new ProducerRecord<>("persistent://public/default/hello-world", "hello-" + idx));

            final RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println("Send hello-" + idx + " to " + recordMetadata);
        }

    }
}
