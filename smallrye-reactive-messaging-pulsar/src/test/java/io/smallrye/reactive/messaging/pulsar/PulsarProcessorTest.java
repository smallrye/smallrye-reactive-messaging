package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarProcessorTest extends WeldTestBase {

    private static final int NUMBER_OF_MESSAGES = 100;

    @Test
    void testPayloadProcessor() throws PulsarClientException {
        // Run app
        ProcessorApp app = runApplication(config(), ProcessorApp.class);

        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // create consumer
        Consumer<Person> consumer = client.newConsumer(Schema.JSON(Person.class))
                .topic(topic + "-sink")
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
        Assertions.assertThat(received.stream().map(p -> p.age))
                .containsAll(IntStream.range(0, 100).boxed().collect(Collectors.toList()));
    }

    @Test
    void testMessageProcessor() throws PulsarClientException {
        // Run app
        MessageProcessorApp app = runApplication(config(), MessageProcessorApp.class);

        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // create consumer
        Consumer<Person> consumer = client.newConsumer(Schema.JSON(Person.class))
                .topic(topic + "-sink")
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
        Assertions.assertThat(received.stream().map(p -> p.age))
                .containsAll(IntStream.range(0, 100).boxed().collect(Collectors.toList()));

        PulsarClientService clientService = get(PulsarClientService.class);
        PulsarClient sinkClient = clientService.getClient("sink");
        PulsarClient dataClient = clientService.getClient("data");
        assertThat(sinkClient).isNotNull().isEqualTo(dataClient);
        assertThat(clientService.getConsumer("data")).isNotNull();
        assertThat(clientService.getProducer("sink")).isNotNull();
        assertThat(clientService.getProducerChannels()).containsExactly("sink");
        assertThat(clientService.getConsumerChannels()).containsExactly("data");
        assertThatThrownBy(() -> clientService.getConsumer("not-found").isConnected())
                .isInstanceOf(NullPointerException.class);
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "INT32")
                .with("mp.messaging.outgoing.sink.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.sink.topic", topic + "-sink");
    }

    @ApplicationScoped
    public static class ProcessorApp {

        @Produces
        @Identifier("sink")
        static Schema<Person> schema = Schema.JSON(Person.class);

        @Incoming("data")
        @Outgoing("sink")
        public Person produce(int payload) {
            return new Person("p" + payload, payload);
        }
    }

    @ApplicationScoped
    public static class MessageProcessorApp {
        @Produces
        @Identifier("sink")
        static Schema<Person> schema = Schema.JSON(Person.class);

        @Incoming("data")
        @Outgoing("sink")
        public Message<Person> produce(Message<Integer> msg) {
            Integer payload = msg.getPayload();
            return msg.withPayload(new Person("p" + payload, payload));
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
