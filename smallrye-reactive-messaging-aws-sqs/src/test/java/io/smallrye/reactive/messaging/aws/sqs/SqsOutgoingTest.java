package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.json.jackson.JacksonMapping;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;

class SqsOutgoingTest extends SqsTestBase {

    @Test
    void testProducer() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.queue", queue);

        String queueUrl = createQueue(queue);
        var app = runApplication(config, ProducerApp.class);
        int expected = 10;
        List<Message> messages = receiveMessages(queueUrl, expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected);
    }

    @ApplicationScoped
    public static class ProducerApp {

        private static final AtomicInteger COUNTER = new AtomicInteger(0);

        @Outgoing("data")
        public Uni<String> send() {
            if (COUNTER.getAndIncrement() > 9) {
                return Uni.createFrom().nullItem();
            }
            return Uni.createFrom().item(String.format("hello-%d", COUNTER.get()));
        }
    }

    @Test
    void testProducerInteger() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.queue", queue);

        String queueUrl = createQueue(queue);
        var app = runApplication(config, ProducerIntegerApp.class);
        int expected = 10;
        List<Message> messages = receiveMessages(queueUrl, expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected)
                .extracting(Message::body)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @ApplicationScoped
    public static class ProducerIntegerApp {

        @Outgoing("data")
        public Multi<Integer> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> i);
        }
    }

    @Test
    void testProducerJson() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        addBeans(JacksonMapping.class, ObjectMapperProvider.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.queue", queue);

        String queueUrl = createQueue(queue);
        var app = runApplication(config, ProducerJsonApp.class);
        int expected = 10;
        List<Message> messages = receiveMessages(queueUrl, expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected)
                .extracting(Message::body)
                .allSatisfy(body -> assertThat(body).startsWith("{\"name\":\"person-").endsWith("}"));
    }

    @ApplicationScoped
    public static class ProducerJsonApp {

        @Outgoing("data")
        public Multi<Person> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> new Person("person-" + i, i));
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

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

    @ApplicationScoped
    public static class ObjectMapperProvider {
        @Produces
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }

    @Test
    void testProducerByteArray() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.queue", queue);

        String queueUrl = createQueue(queue);
        var app = runApplication(config, ProducerByteArrayApp.class);
        int expected = 10;
        List<Message> messages = receiveMessages(queueUrl, expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected)
                .extracting(Message::body)
                .allSatisfy(body -> assertThat(body).startsWith("hello-"));
    }

    @ApplicationScoped
    public static class ProducerByteArrayApp {

        @Outgoing("data")
        public Multi<byte[]> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> ("hello-" + i).getBytes());
        }
    }

    @Test
    void testProducerJsonWithoutMapper() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.queue", queue);

        String queueUrl = createQueue(queue);
        ProducerJsonApp app = runApplication(config, ProducerJsonApp.class);
        int expected = 10;
        List<Message> messages = receiveMessages(queueUrl, expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected)
                .extracting(Message::body)
                .allSatisfy(body -> assertThat(body).startsWith("{\"name\":\"person-").endsWith("}"));
    }

}
