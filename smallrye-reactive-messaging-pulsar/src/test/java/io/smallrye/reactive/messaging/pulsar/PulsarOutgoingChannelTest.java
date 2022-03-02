package io.smallrye.reactive.messaging.pulsar;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarOutgoingChannelTest extends PulsarBaseTest {

    private static final int NUMBER_OF_MESSAGES = 1000;

    @Test
    void testOutgoingChannel() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<String>> messages = new CopyOnWriteArrayList<>();

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config()
                .with("max-pending-messages", 10));
        PulsarOutgoingChannel<String> channel = new PulsarOutgoingChannel<>(client, Schema.STRING, oc);

        Subscriber<? super Message<?>> subscriber = channel.getSubscriber().build();

        receive(client.newConsumer(Schema.STRING)
                .consumerName("test-consumer")
                .subscriptionName("subscription")
                .topic(topic)
                .subscribe(), NUMBER_OF_MESSAGES, messages::add);

        Multi.createFrom().range(0, NUMBER_OF_MESSAGES).map(i -> Message.of("v-" + i))
                .subscribe(subscriber);

        await().until(() -> messages.size() == NUMBER_OF_MESSAGES);
    }

    @Test
    void testOutgoingChannelJsonSchema() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<Person>> messages = new CopyOnWriteArrayList<>();

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config()
                .with("max-pending-messages", 10));
        PulsarOutgoingChannel<Person> channel = new PulsarOutgoingChannel<>(client, Schema.JSON(Person.class), oc);

        Subscriber<? super Message<?>> subscriber = channel.getSubscriber().build();

        receive(client.newConsumer(Schema.JSON(Person.class))
                .consumerName("test-consumer")
                .subscriptionName("subscription")
                .topic(topic)
                .subscribe(), NUMBER_OF_MESSAGES, messages::add);

        Multi.createFrom().range(0, NUMBER_OF_MESSAGES).map(i -> Message.of(new Person("name-" + i, i)))
                .subscribe(subscriber);

        await().until(() -> messages.size() == NUMBER_OF_MESSAGES);
    }

    private MapBasedConfig config() {
        return baseConfig()
                .with("channel-name", "channel")
                .with("topic", topic);
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

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
