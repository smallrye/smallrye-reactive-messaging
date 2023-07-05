package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarProducerTest extends WeldTestBase {

    private static final int NUMBER_OF_MESSAGES = 100;

    @Test
    void testPayloadProducer() throws PulsarClientException {
        // Run app
        ProducingApp producingApp = runApplication(config(), ProducingApp.class);

        // create consumer
        Consumer<Person> consumer = client.newConsumer(Schema.JSON(Person.class))
                .topic(topic)
                .subscriptionName("test-" + topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscribe();

        // gather consumes messages
        List<Person> received = new CopyOnWriteArrayList<>();
        receive(consumer, NUMBER_OF_MESSAGES, message -> {
            try {
                received.add(message.getValue());
                consumer.acknowledge(message);
            } catch (Exception e) {
                consumer.negativeAcknowledge(message);
            }
        });

        // wait until we have gathered all the expected messages
        await().until(() -> received.size() >= NUMBER_OF_MESSAGES);
        assertThat(received.stream().map(p -> p.age))
                .containsAll(IntStream.range(0, 100).boxed().collect(Collectors.toList()));
    }

    @Test
    void testOutgoingMessageProducer() throws PulsarClientException {
        // Run app
        OutgoingMessageProducingApp producingApp = runApplication(config(), OutgoingMessageProducingApp.class);

        // create consumer
        Consumer<Person> consumer = client.newConsumer(Schema.JSON(Person.class))
                .topic(topic)
                .subscriptionName("test-" + topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscribe();

        // gather consumes messages
        List<org.apache.pulsar.client.api.Message<Person>> received = new CopyOnWriteArrayList<>();
        receive(consumer, NUMBER_OF_MESSAGES, message -> {
            try {
                received.add(message);
                consumer.acknowledge(message);
            } catch (Exception e) {
                consumer.negativeAcknowledge(message);
            }
        });

        // wait until we have gathered all the expected messages
        await().until(() -> received.size() >= NUMBER_OF_MESSAGES);
        assertThat(received).extracting(m -> m.getValue().age)
                .containsAll(IntStream.range(0, 100).boxed().collect(Collectors.toList()));
        assertThat(received)
                .allMatch(org.apache.pulsar.client.api.Message::hasKey)
                .extracting(m -> Integer.parseInt(m.getKey()))
                .containsAll(IntStream.range(0, 100).boxed().collect(Collectors.toList()));

    }

    @Test
    void testMessageProducer() throws PulsarClientException {
        // Run app
        MessageProducerApp producingApp = runApplication(config(), MessageProducerApp.class);

        // create consumer
        Consumer<Person> consumer = client.newConsumer(Schema.JSON(Person.class))
                .topic(topic)
                .subscriptionName("test-" + topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscribe();

        // gather consumes messages
        List<Person> received = new CopyOnWriteArrayList<>();
        receive(consumer, NUMBER_OF_MESSAGES, message -> {
            try {
                received.add(message.getValue());
                consumer.acknowledge(message);
            } catch (Exception e) {
                consumer.negativeAcknowledge(message);
            }
        });

        // wait until we have gathered all the expected messages
        await().until(() -> received.size() >= NUMBER_OF_MESSAGES);
        assertThat(received.stream().map(p -> p.age))
                .containsAll(IntStream.range(0, 100).boxed().collect(Collectors.toList()));
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.data.topic", topic);
    }

    @ApplicationScoped
    public static class ProducingApp {

        @Produces
        @Identifier("data")
        static Schema<Person> schema = Schema.JSON(Person.class);

        @Outgoing("data")
        public Multi<Person> produce() {
            return Multi.createFrom().range(0, NUMBER_OF_MESSAGES).map(i -> new Person("p" + i, i));
        }
    }

    @ApplicationScoped
    public static class OutgoingMessageProducingApp {

        @Produces
        @Identifier("data")
        static Schema<Person> schema = Schema.JSON(Person.class);

        @Outgoing("data")
        public Multi<OutgoingMessage<Person>> produce() {
            return Multi.createFrom().range(0, NUMBER_OF_MESSAGES)
                    .map(i -> OutgoingMessage.of(String.valueOf(i), new Person("p" + i, i)));
        }
    }

    @ApplicationScoped
    public static class MessageProducerApp {
        @Produces
        @Identifier("data")
        static Schema<Person> schema = Schema.JSON(Person.class);

        @Outgoing("data")
        public Multi<Message<Person>> produce() {
            return Multi.createFrom().range(0, NUMBER_OF_MESSAGES)
                    .map(i -> new Person("p" + i, i))
                    .map(p -> PulsarMessage.of(p, p.name));
        }
    }

    static class Person {
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
