package streamnative.io;

import io.streamnative.util.PropertyLoader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public class HelloAdmin {

    public static void main(String[] args) throws Exception {
        final Properties props = PropertyLoader.loadConfig(args[0]);

        AdminClient client = AdminClient.create(props);
        ListTopicsResult topics = client.listTopics();

        Set<String> x = topics.names().get();

        for (String name: x.stream().toList()) {
            System.out.println(name);
        }

    }
}
