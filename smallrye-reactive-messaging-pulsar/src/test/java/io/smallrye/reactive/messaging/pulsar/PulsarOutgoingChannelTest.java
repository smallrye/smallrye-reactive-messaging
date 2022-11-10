package io.smallrye.reactive.messaging.pulsar;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.pulsar.base.PulsarBaseTest;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarOutgoingChannelTest extends PulsarBaseTest {

    private static final int NUMBER_OF_MESSAGES = 1000;

    @Test
    void testOutgoingChannel() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<String>> messages = new CopyOnWriteArrayList<>();

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config()
                .with("maxPendingMessagesAcrossPartitions", 10));
        PulsarOutgoingChannel<String> channel = new PulsarOutgoingChannel<>(client, Schema.STRING, oc, configResolver);

        Flow.Subscriber<? extends Message<?>> subscriber = channel.getSubscriber();

        receive(client.newConsumer(Schema.STRING)
                .consumerName("test-consumer")
                .subscriptionName("subscription")
                .topic(topic)
                .subscribe(), NUMBER_OF_MESSAGES, messages::add);

        Multi.createFrom().range(0, NUMBER_OF_MESSAGES)
                .map(i -> Message.of("v-" + i))
                .subscribe((Flow.Subscriber<? super Message<String>>) subscriber);

        await().until(() -> messages.size() == NUMBER_OF_MESSAGES);
    }

    @Test
    void testOutgoingChannelJsonSchema() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<Person>> messages = new CopyOnWriteArrayList<>();

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config()
                .with("maxPendingMessagesAcrossPartitions", 10));
        PulsarOutgoingChannel<Person> channel = new PulsarOutgoingChannel<>(client, Schema.JSON(Person.class), oc,
                configResolver);

        Flow.Subscriber<? extends Message<?>> subscriber = channel.getSubscriber();

        receive(client.newConsumer(Schema.JSON(Person.class))
                .consumerName("test-consumer")
                .subscriptionName("subscription")
                .topic(topic)
                .subscribe(), NUMBER_OF_MESSAGES, messages::add);

        Multi.createFrom().range(0, NUMBER_OF_MESSAGES)
                .map(i -> Message.of(new Person("name-" + i, i)))
                .subscribe((Flow.Subscriber<? super Message<Person>>) subscriber);

        await().until(() -> messages.size() == NUMBER_OF_MESSAGES);
    }

    private MapBasedConfig config() {
        return baseConfig()
                .with("channel-name", "channel")
                .with("topicName", topic);
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
