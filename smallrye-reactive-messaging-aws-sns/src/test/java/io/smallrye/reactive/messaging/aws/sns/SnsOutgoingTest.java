package io.smallrye.reactive.messaging.aws.sns;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.json.jackson.JacksonMapping;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SnsOutgoingTest extends SnsTestBase {

    @Test
    void should_use_provided_client_to_send_string_messages() {
        // given
        var config = initClientViaProvider();
        var subscription = createTopicWithQueueSubscription(topic);
        var expected = 10;

        // when
        runApplication(config, ProducerApp.class);

        // then
        var messages = receiveAndDeleteMessages(subscription.queueUrl(), expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected);
    }

    @ApplicationScoped
    public static class ProducerApp {

        @Outgoing("data")
        public Multi<String> send() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> String.format("hello-%d", i));
        }
    }

    @Test
    void should_use_provided_client_to_send_integer_messages() {
        // given
        var config = initClientViaProvider();
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 10;

        // when
        runApplication(config, ProducerIntegerApp.class);

        // then
        List<Message> messages = receiveAndDeleteMessages(subscription.queueUrl(), expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected)
                .extracting(Message::body)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
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
    void should_use_provided_client_to_send_json_messages_with_mapper() {
        // given
        var config = initClientViaProvider();
        addBeans(JacksonMapping.class, ObjectMapperProvider.class);
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 10;

        // when
        runApplication(config, ProducerJsonApp.class);

        // then
        List<Message> messages = receiveAndDeleteMessages(subscription.queueUrl(),
                r -> r.messageAttributeNames(SnsConnector.CLASS_NAME_ATTRIBUTE), expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected)
                .allSatisfy(m -> m.messageAttributes().containsKey(SnsConnector.CLASS_NAME_ATTRIBUTE))
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

    @Test
    void should_use_provided_client_to_send_json_messages_without_mapper() {
        // given
        var config = initClientViaProvider();
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 10;

        // when
        runApplication(config, ProducerJsonApp.class);

        // then
        List<Message> messages = receiveAndDeleteMessages(subscription.queueUrl(), expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected)
                .extracting(Message::body)
                .allSatisfy(body -> assertThat(body).startsWith("{\"name\":\"person-").endsWith("}"));
    }

    @Test
    void should_use_provided_client_to_send_byte_messages() {
        // given
        var config = initClientViaProvider();
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 10;

        // when
        runApplication(config, ProducerByteArrayApp.class);

        // then
        List<Message> messages = receiveAndDeleteMessages(subscription.queueUrl(), expected, Duration.ofSeconds(10));
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

    @ApplicationScoped
    public static class ObjectMapperProvider {
        @Produces
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }

    private MapBasedConfig initClientViaProvider() {
        SnsTestClientProvider.client = getSnsClient();
        addBeans(SnsTestClientProvider.class);
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.topic", topic)
                .with("mp.messaging.outgoing.data.topic.arn", topicArn);
    }
}
