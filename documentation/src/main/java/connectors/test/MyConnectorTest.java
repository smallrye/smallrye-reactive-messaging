package connectors.test;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import connectors.MyConnector;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MyConnectorTest extends WeldTestBase {

    @Test
    void incomingChannel() {
        String host = "";
        int port = 0;
        String myTopic = UUID.randomUUID().toString();
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.topic", myTopic)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .with("mp.messaging.incoming.data.connector", MyConnector.CONNECTOR_NAME);
        MyApp app = runApplication(config, MyApp.class);

        int expected = 10;
        // produce expected number of messages to myTopic

        // wait until app received
        await().until(() -> app.received().size() == expected);
    }

    @ApplicationScoped
    public static class MyApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(String msg) {
            received.add(msg);
        }

        public List<String> received() {
            return received;
        }
    }
}
