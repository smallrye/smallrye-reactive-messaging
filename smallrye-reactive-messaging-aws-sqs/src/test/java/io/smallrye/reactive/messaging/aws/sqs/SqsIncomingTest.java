package io.smallrye.reactive.messaging.aws.sqs;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.smallrye.reactive.messaging.json.jackson.JacksonMapping;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.Json;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

class SqsIncomingTest extends SqsTestBase {

    @Test
    void testConsumer() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue);

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().until(() -> app.received().size() == expected);
    }

    @ApplicationScoped
    public static class ConsumerApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(String msg) {
            received.add(msg);
        }

        public List<String> received() {
            return received;
        }
    }

    @Test
    void testConsumerInteger() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                        .dataType("String").stringValue(Integer.class.getName()).build())));
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue);

        ConsumerIntegerApp app = runApplication(config, ConsumerIntegerApp.class);
        await().until(() -> app.received().size() == expected);
    }

    @ApplicationScoped
    public static class ConsumerIntegerApp {

        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(int msg) {
            received.add(msg);
        }

        public List<Integer> received() {
            return received;
        }
    }

    @Test
    void testConsumerJson() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        addBeans(JacksonMapping.class, ObjectMapperProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected, (i, r) -> {
            Person person = new Person();
            person.name = "person-" + i;
            person.age = i;
            r.messageBody(Json.encode(person))
                    .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                            .dataType("String").stringValue(Person.class.getName()).build()));
        });
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue);

        ConsumerJsonApp app = runApplication(config, ConsumerJsonApp.class);
        await().until(() -> app.received().size() == expected);
    }

    @ApplicationScoped
    public static class ConsumerJsonApp {

        List<Person> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(Person msg) {
            received.add(msg);
        }

        public List<Person> received() {
            return received;
        }

    }

    public static class Person {
        public String name;
        public int age;

    }

    @ApplicationScoped
    public static class ObjectMapperProvider {
        @Produces
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }

}
