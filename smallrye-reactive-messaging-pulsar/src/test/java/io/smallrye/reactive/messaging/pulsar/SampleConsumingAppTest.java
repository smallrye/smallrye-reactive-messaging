package io.smallrye.reactive.messaging.pulsar;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SampleConsumingAppTest extends WeldTestBase {

    public static final int NUMBER_OF_MESSAGES = 100;

    @Test
    void testAppUsingPulsarConnector() throws PulsarClientException {
        // Run app
        ConsumingApp app = runApplication(config(), ConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().atMost(Duration.ofSeconds(30)).until(() -> app.getResults().size() == NUMBER_OF_MESSAGES);
    }

    @Test
    void testAppUsingPulsarConnectorBlocking() throws PulsarClientException {
        // Run app
        BlockingConsumingApp app = runApplication(config(), BlockingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer-2")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().atMost(Duration.ofSeconds(30)).until(() -> app.getResults().size() == NUMBER_OF_MESSAGES);
    }

    MapBasedConfig config() {
        // TODO complete config
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.service-url", serviceUrl)
                .with("mp.messaging.incoming.data.subscription.type", "Failover")
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "INT32");
    }

    @ApplicationScoped
    public static class ConsumingApp {

        private final List<Integer> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> message) {
            results.add(message.getPayload());
            return message.ack();
        }

        public List<Integer> getResults() {
            return results;
        }
    }

    @ApplicationScoped
    public static class BlockingConsumingApp {

        private final List<Integer> results = new CopyOnWriteArrayList<>();

        @Blocking
        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> message) {
            results.add(message.getPayload());
            return message.ack();
        }

        public List<Integer> getResults() {
            return results;
        }
    }
}
