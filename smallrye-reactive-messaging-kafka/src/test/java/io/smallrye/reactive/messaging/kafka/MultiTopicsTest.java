package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

/**
 * Test the Incoming connector when multiple topics are used either using a pattern or a list of topics.
 */
@SuppressWarnings("rawtypes")
public class MultiTopicsTest extends KafkaTestBase {

    @RepeatedTest(5)
    public void testWithThreeTopicsInConfiguration() {
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String topic3 = UUID.randomUUID().toString();

        KafkaConsumer bean = runApplication(KafkaMapBasedConfig.builder("mp.messaging.incoming.kafka")
                .put(
                        "value.deserializer", StringDeserializer.class.getName(),
                        "topics", topic1 + ", " + topic2 + ", " + topic3,
                        "auto.offset.reset", "earliest")
                .build(), KafkaConsumer.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(bean.getMessages()).isEmpty();

        AtomicInteger key = new AtomicInteger();
        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic1, Integer.toString(key.getAndIncrement()), "hello"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic2, Integer.toString(key.getAndIncrement()), "hallo"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic3, Integer.toString(key.getAndIncrement()), "bonjour"))).start();

        await().until(() -> bean.getMessages().size() >= 9);

        AtomicInteger top1 = new AtomicInteger();
        AtomicInteger top2 = new AtomicInteger();
        AtomicInteger top3 = new AtomicInteger();
        bean.getMessages().forEach(message -> {
            // TODO Import normally once the deprecated copy in this package has gone
            io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata record = message
                    .getMetadata(io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata.class).orElse(null);
            assertThat(record).isNotNull();
            String topic = record.getTopic();
            if (topic.equals(topic1)) {
                top1.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hello");
            } else if (topic.equals(topic2)) {
                top2.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hallo");
            } else if (topic.equals(topic3)) {
                top3.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("bonjour");
            }
            LegacyMetadataTestUtils.tempCompareLegacyAndApiMetadata(record, message);
        });

        assertThat(top1.get()).isGreaterThanOrEqualTo(3);
        assertThat(top2.get()).isGreaterThanOrEqualTo(3);
        assertThat(top3.get()).isGreaterThanOrEqualTo(3);
    }

    @RepeatedTest(5)
    public void testWithOnlyTwoTopicsReceiving() {
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String topic3 = UUID.randomUUID().toString();

        KafkaConsumer bean = runApplication(KafkaMapBasedConfig.builder("mp.messaging.incoming.kafka")
                .put(
                        "value.deserializer", StringDeserializer.class.getName(),
                        "topics", topic1 + ", " + topic2 + ", " + topic3,
                        "graceful-shutdown", false,
                        "auto.offset.reset", "earliest")
                .build(), KafkaConsumer.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(bean.getMessages()).isEmpty();

        AtomicInteger key = new AtomicInteger();
        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic1, Integer.toString(key.incrementAndGet()), "hello"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic3, Integer.toString(key.incrementAndGet()), "bonjour"))).start();

        await().until(() -> bean.getMessages().size() >= 6);

        AtomicInteger top1 = new AtomicInteger();
        AtomicInteger top2 = new AtomicInteger();
        AtomicInteger top3 = new AtomicInteger();
        bean.getMessages().forEach(message -> {
            io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata record = message
                    .getMetadata(io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata.class).orElse(null);
            assertThat(record).isNotNull();
            String topic = record.getTopic();
            if (topic.equals(topic1)) {
                top1.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hello");
            } else if (topic.equals(topic2)) {
                top2.incrementAndGet();
            } else if (topic.equals(topic3)) {
                top3.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("bonjour");
            }
            LegacyMetadataTestUtils.tempCompareLegacyAndApiMetadata(record, message);
        });

        assertThat(top1).hasValue(3);
        assertThat(top2).hasValue(0);
        assertThat(top3).hasValue(3);
    }

    @Test
    public void testWithPattern() {
        String topic1 = "greetings-" + UUID.randomUUID().toString();
        String topic2 = "greetings-" + UUID.randomUUID().toString();
        String topic3 = "greetings-" + UUID.randomUUID().toString();

        createTopic(topic1, 1);
        createTopic(topic2, 1);
        createTopic(topic3, 1);

        KafkaConsumer bean = runApplication(KafkaMapBasedConfig.builder("mp.messaging.incoming.kafka")
                .put(
                        "value.deserializer", StringDeserializer.class.getName(),
                        "topic", "greetings-.+",
                        "pattern", true,
                        "auto.offset.reset", "earliest")
                .build(), KafkaConsumer.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(bean.getMessages()).isEmpty();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic1, "hello"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic2, "hallo"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>(topic3, "bonjour"))).start();

        new Thread(() -> usage.produceStrings(3, null,
                () -> new ProducerRecord<>("do-not-match", "Bahh!"))).start();

        await().until(() -> bean.getMessages().size() >= 9);

        AtomicInteger top1 = new AtomicInteger();
        AtomicInteger top2 = new AtomicInteger();
        AtomicInteger top3 = new AtomicInteger();
        bean.getMessages().forEach(message -> {
            IncomingKafkaRecordMetadata record = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
            assertThat(record).isNotNull();
            String topic = record.getTopic();
            if (topic.equals(topic1)) {
                top1.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hello");
            } else if (topic.equals(topic2)) {
                top2.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("hallo");
            } else if (topic.equals(topic3)) {
                top3.incrementAndGet();
                assertThat(message.getPayload()).isEqualTo("bonjour");
            }
            LegacyMetadataTestUtils.tempCompareLegacyAndApiMetadata(record, message);
        });

        assertThat(top1).hasValue(3);
        assertThat(top2).hasValue(3);
        assertThat(top3).hasValue(3);
    }

    @Test
    public void testNonReadinessWithPatternIfTopicsAreNotCreated() {
        runApplication(KafkaMapBasedConfig.builder("mp.messaging.incoming.kafka")
                .put(
                        "value.deserializer", StringDeserializer.class.getName(),
                        "topic", "greetings-.+",
                        "pattern", true,
                        "auto.offset.reset", "earliest",
                        "health-readiness-topic-verification", true)
                .build(), KafkaConsumer.class);

        await().until(this::isAlive);
        await()
                .pollDelay(10, TimeUnit.MILLISECONDS)
                .until(() -> !isReady());

    }

    @Test
    public void testInvalidConfigurations() {
        // Pattern and no topic
        assertThatThrownBy(() -> runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.kafka.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.kafka.pattern", true), KafkaConsumer.class))
                        .isInstanceOf(DeploymentException.class)
                        .hasCauseInstanceOf(IllegalArgumentException.class);

        // topics and no topic
        assertThatThrownBy(() -> runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.kafka.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.kafka.topic", "my-topic")
                .with("mp.messaging.incoming.kafka.topics", "a, b, c"), KafkaConsumer.class))
                        .isInstanceOf(DeploymentException.class)
                        .hasCauseInstanceOf(IllegalArgumentException.class);

        // topics and pattern
        assertThatThrownBy(() -> runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.kafka.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.kafka.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.kafka.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.kafka.pattern", true)
                .with("mp.messaging.incoming.kafka.topics", "a, b, c"), KafkaConsumer.class))
                        .isInstanceOf(DeploymentException.class)
                        .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @ApplicationScoped
    public static class KafkaConsumer {

        private final List<Message<String>> messages = new CopyOnWriteArrayList<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<String> incoming) {
            messages.add(incoming);
            return incoming.ack();
        }

        public List<Message<String>> getMessages() {
            return messages;
        }

    }

}
