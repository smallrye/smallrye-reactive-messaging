package io.smallrye.reactive.messaging.pulsar.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.pulsar.PulsarBatchMessage;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.ack.PulsarCumulativeAck;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Reproducer for https://github.com/smallrye/smallrye-reactive-messaging/issues/3365:
 * a payload that cannot be decoded with the channel's JSON schema must be routed to the
 * configured failure strategy instead of breaking the stream.
 */
public class PulsarInvalidPayloadTest extends WeldTestBase {

    private static final byte[] INVALID_JSON = "{not valid json".getBytes(StandardCharsets.UTF_8);

    @Test
    void ignoreSkipsInvalidPayloadAndKeepsConsuming() throws PulsarClientException {
        addBeans(PulsarIgnore.Factory.class, PersonSchema.class);
        ConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore"), ConsumingApp.class);
        await().until(this::isReady);

        sendValidInvalidValid();

        await().untilAsserted(() -> assertThat(app.getResults()).containsExactly("before", "after"));
        assertThat(isAlive()).isTrue();
    }

    @Test
    void ignoreSkipsInvalidPayloadInBatch() throws PulsarClientException {
        addBeans(PulsarIgnore.Factory.class, PersonSchema.class);
        BatchConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore")
                .with("mp.messaging.incoming.data.batchReceive", true), BatchConsumingApp.class);
        await().until(this::isReady);

        sendValidInvalidValid();

        await().untilAsserted(() -> assertThat(app.getResults()).containsExactly("before", "after"));
        assertThat(isAlive()).isTrue();
    }

    @Test
    void nackSendsInvalidPayloadToDeadLetterTopic() throws PulsarClientException {
        addBeans(PersonSchema.class);
        ConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "nack")
                .with("mp.messaging.incoming.data.subscriptionType", "Shared")
                .with("mp.messaging.incoming.data.negativeAckRedeliveryDelayMicros", 100)
                .with("mp.messaging.incoming.data.deadLetterPolicy.maxRedeliverCount", 2)
                .with("mp.messaging.incoming.data.deadLetterPolicy.deadLetterTopic", topic + "-dlq")
                .with("mp.messaging.incoming.data.deadLetterPolicy.initialSubscriptionName", "initial-dlq-sub"),
                ConsumingApp.class);
        await().until(this::isReady);

        sendValidInvalidValid();

        await().untilAsserted(() -> assertThat(app.getResults()).containsExactlyInAnyOrder("before", "after"));

        List<Message<byte[]>> deadLetters = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.BYTES)
                .topic(topic + "-dlq")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("initial-dlq-sub")
                .subscribe(), 1, deadLetters::add);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(deadLetters).extracting(Message::getData).containsExactly(INVALID_JSON));
        assertThat(app.getResults()).hasSize(2);
        assertThat(isAlive()).isTrue();
    }

    @Test
    void failStrategyStopsChannelOnInvalidPayload() throws PulsarClientException {
        addBeans(PulsarFailStop.Factory.class, PersonSchema.class);
        ConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "fail"), ConsumingApp.class);
        await().until(this::isReady);

        sendInvalid();
        sendValid("after");

        await().until(() -> !isAlive());
        await().pollDelay(1, TimeUnit.SECONDS).untilAsserted(() -> assertThat(app.getResults()).isEmpty());
    }

    /**
     * With cumulative acks, ignoring an invalid message must not acknowledge earlier valid messages
     * that the application has not acknowledged yet.
     */
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void ignoreWithCumulativeAckDoesNotAckUnprocessedMessages(boolean batch) throws PulsarClientException {
        addBeans(PulsarIgnore.Factory.class, PulsarCumulativeAck.Factory.class, PersonSchema.class);
        // Publish before the app subscribes so batch receive gets all three messages at once.
        sendValidInvalidValid();

        String subscription = "cumulative-" + batch;
        MapBasedConfig config = config()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore")
                .with("mp.messaging.incoming.data.ack-strategy", "cumulative")
                .with("mp.messaging.incoming.data.subscriptionName", subscription);
        List<String> received;
        if (batch) {
            received = runApplication(config.with("mp.messaging.incoming.data.batchReceive", true),
                    NonAckingBatchApp.class).getResults();
        } else {
            received = runApplication(config, NonAckingApp.class).getResults();
        }
        await().untilAsserted(() -> assertThat(received).containsExactly("before", "after"));

        // Simulate a crash: stop the app without acknowledging the valid messages.
        container.close();
        container = null;

        List<String> redelivered = new CopyOnWriteArrayList<>();
        receive(client.newConsumer(Schema.JSON(Person.class))
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe(), 2, m -> redelivered.add(m.getValue().name));
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(redelivered).containsExactly("before", "after"));
    }

    private void sendValidInvalidValid() throws PulsarClientException {
        sendValid("before");
        sendInvalid();
        sendValid("after");
    }

    private void sendValid(String name) throws PulsarClientException {
        try (Producer<Person> producer = client.newProducer(Schema.JSON(Person.class))
                .topic(topic)
                .enableBatching(false)
                .create()) {
            producer.send(new Person(name, 1));
        }
    }

    private void sendInvalid() throws PulsarClientException {
        // A schema-less producer bypasses JSON validation, simulating a misbehaving publisher.
        try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(topic)
                .enableBatching(false)
                .create()) {
            producer.send(INVALID_JSON);
        }
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest);
    }

    @ApplicationScoped
    public static class PersonSchema {
        @Produces
        @Identifier("data")
        static Schema<Person> schema = Schema.JSON(Person.class);
    }

    @ApplicationScoped
    public static class ConsumingApp {

        private final List<String> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Person person) {
            results.add(person.name);
        }

        public List<String> getResults() {
            return results;
        }
    }

    @ApplicationScoped
    public static class BatchConsumingApp {

        private final List<String> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(List<Person> persons) {
            persons.forEach(p -> results.add(p.name));
        }

        public List<String> getResults() {
            return results;
        }
    }

    @ApplicationScoped
    public static class NonAckingApp {

        private final List<String> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(PulsarMessage<Person> message) {
            results.add(message.getPayload().name);
            return CompletableFuture.completedFuture(null);
        }

        public List<String> getResults() {
            return results;
        }
    }

    @ApplicationScoped
    public static class NonAckingBatchApp {

        private final List<String> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(PulsarBatchMessage<Person> batch) {
            batch.getPayload().forEach(p -> results.add(p.name));
            return CompletableFuture.completedFuture(null);
        }

        public List<String> getResults() {
            return results;
        }
    }

    public static class Person {
        public String name;
        public int age;

        public Person() {
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
