package io.smallrye.reactive.messaging.pulsar.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
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

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Reproducer for https://github.com/smallrye/smallrye-reactive-messaging/issues/3365:
 * a payload that cannot be decoded with the channel's schema must be routed to the
 * configured failure strategy instead of breaking the stream.
 * <p>
 * Adapted from the tests of https://github.com/smallrye/smallrye-reactive-messaging/pull/3606.
 */
public class PulsarInvalidPayloadTest extends WeldTestBase {

    private static final byte[] INVALID_JSON = "{not valid json".getBytes(StandardCharsets.UTF_8);
    // An incomplete Avro varint: the decoder hits the end of the input.
    private static final byte[] INVALID_AVRO = new byte[] { (byte) 0xFF };

    @Test
    void ignoreSkipsInvalidPayloadAndKeepsConsuming() throws PulsarClientException {
        addBeans(PulsarIgnore.Factory.class, JsonPersonSchema.class);
        ConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore"), ConsumingApp.class);
        await().until(this::isReady);

        sendValidInvalidValid(Schema.JSON(Person.class), INVALID_JSON);

        await().untilAsserted(() -> assertThat(app.getResults()).containsExactly("before", "after"));
        assertThat(isAlive()).isTrue();
    }

    /**
     * Avro schemas are decoded eagerly on the Pulsar client thread to fetch the schema info.
     */
    @Test
    void ignoreSkipsInvalidAvroPayloadAndKeepsConsuming() throws PulsarClientException {
        addBeans(PulsarIgnore.Factory.class, AvroPersonSchema.class);
        ConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore"), ConsumingApp.class);
        await().until(this::isReady);

        sendValidInvalidValid(Schema.AVRO(Person.class), INVALID_AVRO);

        await().untilAsserted(() -> assertThat(app.getResults()).containsExactly("before", "after"));
        assertThat(isAlive()).isTrue();
    }

    @Test
    void ignoreSkipsInvalidPayloadForMessageConsumer() throws PulsarClientException {
        addBeans(PulsarIgnore.Factory.class, JsonPersonSchema.class);
        MessageConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "ignore"), MessageConsumingApp.class);
        await().until(this::isReady);

        sendValidInvalidValid(Schema.JSON(Person.class), INVALID_JSON);

        await().untilAsserted(() -> assertThat(app.getResults()).containsExactly("before", "after"));
        assertThat(isAlive()).isTrue();
    }

    @Test
    void nackSendsInvalidPayloadToDeadLetterTopic() throws PulsarClientException {
        addBeans(JsonPersonSchema.class);
        ConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "nack")
                .with("mp.messaging.incoming.data.subscriptionType", "Shared")
                .with("mp.messaging.incoming.data.negativeAckRedeliveryDelayMicros", 100)
                .with("mp.messaging.incoming.data.deadLetterPolicy.maxRedeliverCount", 2)
                .with("mp.messaging.incoming.data.deadLetterPolicy.deadLetterTopic", topic + "-dlq")
                .with("mp.messaging.incoming.data.deadLetterPolicy.initialSubscriptionName", "initial-dlq-sub"),
                ConsumingApp.class);
        await().until(this::isReady);

        sendValidInvalidValid(Schema.JSON(Person.class), INVALID_JSON);

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

    /**
     * The fail strategy stops the consumer. Messages already received by the channel before it closed
     * may still be delivered, so only the channel state and the invalid message are checked.
     */
    @Test
    void failStrategyStopsChannelOnInvalidPayload() throws PulsarClientException {
        addBeans(PulsarFailStop.Factory.class, JsonPersonSchema.class);
        ConsumingApp app = runApplication(config()
                .with("mp.messaging.incoming.data.failure-strategy", "fail"), ConsumingApp.class);
        await().until(this::isReady);

        sendValid(Schema.JSON(Person.class), "before");
        sendInvalid(INVALID_JSON);

        await().until(() -> !isAlive());
        assertThat(app.getResults()).containsExactly("before");

        sendValid(Schema.JSON(Person.class), "after");
        await().pollDelay(1, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app.getResults()).containsExactly("before"));
    }

    private void sendValidInvalidValid(Schema<Person> schema, byte[] invalid) throws PulsarClientException {
        sendValid(schema, "before");
        sendInvalid(invalid);
        sendValid(schema, "after");
    }

    private void sendValid(Schema<Person> schema, String name) throws PulsarClientException {
        try (Producer<Person> producer = client.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .create()) {
            producer.send(new Person(name, 1));
        }
    }

    private void sendInvalid(byte[] payload) throws PulsarClientException {
        // A schema-less producer bypasses schema validation, simulating a misbehaving publisher.
        try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(topic)
                .enableBatching(false)
                .create()) {
            producer.send(payload);
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
    public static class JsonPersonSchema {
        @Produces
        @Identifier("data")
        static Schema<Person> schema = Schema.JSON(Person.class);
    }

    @ApplicationScoped
    public static class AvroPersonSchema {
        @Produces
        @Identifier("data")
        static Schema<Person> schema = Schema.AVRO(Person.class);
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
    public static class MessageConsumingApp {

        private final List<String> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(PulsarMessage<Person> message) {
            results.add(message.getPayload().name);
            return message.ack();
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
