package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class PulsarIncomingChannelTest extends PulsarBaseTest {

    private static final int NUMBER_OF_MESSAGES = 1000;

    @Test
    void testIncomingChannel() throws PulsarClientException {
        List<Message<?>> messages = new CopyOnWriteArrayList<>();

        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(config());
        PulsarIncomingChannel<Person> channel = new PulsarIncomingChannel<>(client, Vertx.vertx(), Schema.JSON(Person.class),
                ic);
        Multi.createFrom().publisher(channel.getPublisher().buildRs())
                .subscribe().with(e -> messages.add(e));

        send(client.newProducer(Schema.JSON(Person.class))
                .producerName("test-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, i -> new Person(ThreadLocalRandom.current().nextInt() + "", i));

        await().until(() -> messages.size() == NUMBER_OF_MESSAGES);
        assertThat(messages).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> ((Person) m.getPayload()).age)
                .containsSequence(IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()));
    }

    MapBasedConfig config() {
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
