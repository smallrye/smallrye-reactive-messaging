package io.smallrye.reactive.messaging.pulsar.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarNackTest extends WeldTestBase {

    public static final int NUMBER_OF_MESSAGES = 100;

    @Test
    void testFailStop() throws PulsarClientException {
        addBeans(PulsarFailStop.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "fail"), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSize(0);
            assertThat(app.getFailures()).hasSize(1);
        });
    }

    @Test
    void testIgnore() throws PulsarClientException {
        addBeans(PulsarIgnore.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore"), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSize(NUMBER_OF_MESSAGES - 10);
            assertThat(app.getFailures()).hasSize(10);
        });
    }

    @Test
    void testNackRedelivery() throws PulsarClientException {
        addBeans(PulsarNack.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.negativeAckRedeliveryDelayMicros", "100"), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSizeGreaterThanOrEqualTo(NUMBER_OF_MESSAGES - 10);
            assertThat(app.getFailures()).hasSizeGreaterThanOrEqualTo(10);
        });
    }

    @Test
    void testReconsumeLater() throws PulsarClientException {
        addBeans(PulsarReconsumeLater.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "reconsume-later")
                .with("mp.messaging.incoming.data.deadLetterPolicy.retryLetterTopic", topic + "-retry")
                .with("mp.messaging.incoming.data.deadLetterPolicy.maxRedeliverCount", 2)
                .with("mp.messaging.incoming.data.retryEnable", true)
                .with("mp.messaging.incoming.data.subscriptionType", "Shared"), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSize(NUMBER_OF_MESSAGES - 10);
            assertThat(app.getFailures()).hasSize(30);
        });

        List<Message<Integer>> retries = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .topic(topic + "-retry")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("subscription")
                .subscribe(), 20, retries::add);

        // Check for retried messages
        await().untilAsserted(() -> assertThat(retries).hasSize(20));
    }

    /**
     * TODO Test Disabled
     * To be fixed with Pulsar 3.0.0
     */
    @Test
    @Disabled
    void testDeadLetterTopic() throws PulsarClientException {
        addBeans(PulsarReconsumeLater.Factory.class);
        // Run app
        FailingConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "nack")
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
                .with("mp.messaging.incoming.data.negativeAckRedeliveryDelayMicros", 100)
                .with("mp.messaging.incoming.data.deadLetterPolicy.maxRedeliverCount", 2)
                .with("mp.messaging.incoming.data.deadLetterPolicy.deadLetterTopic", topic + "-dlq")
                .with("mp.messaging.incoming.data.deadLetterPolicy.initialSubscriptionName", "initial-dlq-sub")
                .with("mp.messaging.incoming.data.subscriptionType", "Shared"), FailingConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .enableBatching(false)
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().untilAsserted(() -> {
            assertThat(app.getResults()).hasSize(NUMBER_OF_MESSAGES - 10);
            assertThat(app.getFailures()).hasSizeGreaterThanOrEqualTo(30);
        });

        List<Message<Integer>> retries = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.INT32)
                .topic(topic + "-dlq")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("initial-dlq-sub")
                .subscribe()).subscribe().with(retries::add);

        // Check for retried messages
        await().untilAsserted(() -> assertThat(retries).extracting(Message::getValue).hasSize(10));
    }

    @ApplicationScoped
    public static class FailingConsumingApp {

        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final List<Integer> failures = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(PulsarMessage<Integer> message) {
            Integer payload = message.getPayload();
            if (payload % 10 == 0) {
                failures.add(payload);
                return message.nack(new IllegalArgumentException("boom"));
            }
            results.add(payload);
            return message.ack();
        }

        public List<Integer> getResults() {
            return results;
        }

        public List<Integer> getFailures() {
            return failures;
        }
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "INT32");
    }

}
