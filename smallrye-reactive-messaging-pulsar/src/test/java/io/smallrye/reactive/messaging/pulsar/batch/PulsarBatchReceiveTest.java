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
import jakarta.enterprise.inject.Produces;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessage;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessageMetadata;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessageMetadata;
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
    void testBatchReceiveAppUsingPulsarConnector() {
        // Run app
        ConsumingApp app = runApplication(config(), ConsumingApp.class);
        long start = System.currentTimeMillis();

        // Check for consumed messages in app
        await().atMost(Duration.ofSeconds(30)).until(() -> app.getResults().size() == NUMBER_OF_MESSAGES);
        long end = System.currentTimeMillis();

        System.out.println("Ack - Estimate: " + (end - start) + " ms");
        assertThat(app.getResults()).containsExactlyElementsOf(expected);
    }

    @Test
    void testBatchReceiveAppUsingPayloads() {
        // Run app
        MessagesConsumingApp app = runApplication(config(), MessagesConsumingApp.class);
        long start = System.currentTimeMillis();

        // Check for consumed messages in app
        await().atMost(Duration.ofSeconds(30)).until(() -> app.getResults().size() == NUMBER_OF_MESSAGES);
        long end = System.currentTimeMillis();

        System.out.println("Ack - Estimate: " + (end - start) + " ms");
        assertThat(app.getResults()).containsExactlyElementsOf(expected);
    }

    @Test
    void testBatchReceiveAppWithCustomConfig() {
        addBeans(BatchConfig.class);
        // Run app
        ConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.consumer-configuration", "batch-config"), ConsumingApp.class);
        long start = System.currentTimeMillis();

        // Check for consumed messages in app
        await().atMost(Duration.ofSeconds(30)).until(() -> app.getResults().size() == NUMBER_OF_MESSAGES);
        long end = System.currentTimeMillis();

        System.out.println("Ack - Estimate: " + (end - start) + " ms");
        assertThat(app.getResults()).containsExactlyElementsOf(expected);
    }

    @ApplicationScoped
    public static class BatchConfig {
        @Produces
        @Identifier("batch-config")
        public ConsumerConfigurationData<Object> configureBatchConsumer() {
            var data = new ConsumerConfigurationData<>();
            data.setBatchReceivePolicy(BatchReceivePolicy.builder()
                    .maxNumMessages(10)
                    .build());
            return data;
        }
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
            results.addAll(message.getPayload());
            return message.ack();
        }

        public List<Integer> getResults() {
            return results;
        }
    }

    @ApplicationScoped
    public static class MessagesConsumingApp {

        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final List<PulsarIncomingMessageMetadata> allMetadata = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Messages<Integer> batch, PulsarIncomingBatchMessageMetadata metadata) {
            for (Message<Integer> message : batch) {
                PulsarIncomingMessageMetadata msgMetadata = metadata.getMetadataForMessage(message,
                        PulsarIncomingMessageMetadata.class);
                assertThat(msgMetadata).isNotNull();
                allMetadata.add(msgMetadata);
                results.add(message.getValue());
            }
        }

        public List<Integer> getResults() {
            return results;
        }

        public List<PulsarIncomingMessageMetadata> getAllMetadata() {
            return allMetadata;
        }
    }

}
