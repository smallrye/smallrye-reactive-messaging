package io.smallrye.reactive.messaging.pulsar.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessage;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarBatchReceiveTest extends WeldTestBase {

    public static final int NUMBER_OF_MESSAGES = 1_000;

    public static String topic = UUID.randomUUID().toString();
    private static List<Integer> expected;

    @BeforeAll
    static void insertMessages() throws PulsarClientException {
        expected = new ArrayList<>();
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> {
                    expected.add(i);
                    return i;
                });
    }

    @Test
    void testBatchRecieveAppUsingPulsarConnector() {
        // Run app
        ConsumingApp app = runApplication(config(), ConsumingApp.class);
        long start = System.currentTimeMillis();

        // Check for consumed messages in app
        await().atMost(Duration.ofSeconds(30)).until(() -> app.getResults().size() == NUMBER_OF_MESSAGES);
        long end = System.currentTimeMillis();

        System.out.println("Ack - Estimate: " + (end - start) + " ms");
        assertThat(app.getResults()).containsExactlyElementsOf(expected);
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
                // TODO SubscriptionType.Failover doesn't work on batch receive
                //                .with("mp.messaging.incoming.data.subscriptionType", SubscriptionType.Failover)
                .with("mp.messaging.incoming.data.schema", "INT32")
                .with("mp.messaging.incoming.data.batchReceive", "true");
    }

    @ApplicationScoped
    public static class ConsumingApp {

        private final List<Integer> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(PulsarIncomingBatchMessage<Integer> message) {
            //            System.out.println(message.getIncomingMessages().size());
            results.addAll(message.getPayload());
            return message.ack();
        }

        public List<Integer> getResults() {
            return results;
        }
    }

}
