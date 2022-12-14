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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.companion.ProducerTask;

/**
 * Test the Incoming connector when multiple topics are used either using a pattern or a list of topics.
 */
@SuppressWarnings("rawtypes")
public class MultiTopicsTest extends KafkaCompanionTestBase {

    @RepeatedTest(5)
    public void testWithThreeTopicsInConfiguration() {
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String topic3 = UUID.randomUUID().toString();

        KafkaConsumer bean = runApplication(kafkaConfig("mp.messaging.incoming.kafka")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("topics", topic1 + ", " + topic2 + ", " + topic3)
                .with("auto.offset.reset", "earliest"),
                KafkaConsumer.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(bean.getMessages()).isEmpty();

        companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic1, Integer.toString(i), "hello"), 3);

        companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic2, Integer.toString(i), "hallo"), 3);

        companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic3, Integer.toString(i), "bonjour"), 3);

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

        KafkaConsumer bean = runApplication(kafkaConfig("mp.messaging.incoming.kafka")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("topics", topic1 + ", " + topic2 + ", " + topic3)
                .with("graceful-shutdown", false)
                .with("auto.offset.reset", "earliest"),
                KafkaConsumer.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(bean.getMessages()).isEmpty();

        ProducerTask pt1 = companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic1, Integer.toString(i), "hello"), 3);

        ProducerTask pt2 = companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic3, Integer.toString(i), "bonjour"), 3);

        await().until(() -> pt1.count() == 3 && pt2.count() == 3);
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

        companion.topics().createAndWait(topic1, 1);
        companion.topics().createAndWait(topic2, 1);
        companion.topics().createAndWait(topic3, 1);

        KafkaConsumer bean = runApplication(kafkaConfig("mp.messaging.incoming.kafka")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("topic", "greetings-.+")
                .with("pattern", true)
                .with("auto.offset.reset", "earliest"),
                KafkaConsumer.class);

        await().until(this::isReady);
        await().until(this::isAlive);

        assertThat(bean.getMessages()).isEmpty();

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic1, "hello"), 3);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic2, "hallo"), 3);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic3, "bonjour"), 3);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>("do-not-match", "Bahh!"), 3);

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
        runApplication(kafkaConfig("mp.messaging.incoming.kafka")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("topic", "greetings-.+")
                .with("pattern", true)
                .with("auto.offset.reset", "earliest")
                .with("health-readiness-topic-verification", true),
                KafkaConsumer.class);

        await().until(this::isAlive);
        await()
                .pollDelay(10, TimeUnit.MILLISECONDS)
                .until(() -> !isReady());

    }

    @Test
    public void testInvalidConfigurations() {
        // Pattern and no topic
        assertThatThrownBy(() -> runApplication(kafkaConfig("mp.messaging.incoming.kafka")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("pattern", true),
                KafkaConsumer.class))
                        .isInstanceOf(DeploymentException.class)
                        .hasCauseInstanceOf(IllegalArgumentException.class);

        // topics and no topic
        assertThatThrownBy(() -> runApplication(kafkaConfig("mp.messaging.incoming.kafka")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("topic", "my-topic")
                .with("topics", "a, b, c"), KafkaConsumer.class))
                        .isInstanceOf(DeploymentException.class)
                        .hasCauseInstanceOf(IllegalArgumentException.class);

        // topics and pattern
        assertThatThrownBy(() -> runApplication(kafkaConfig("mp.messaging.incoming.kafka")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("pattern", true)
                .with("topics", "a, b, c"), KafkaConsumer.class))
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
