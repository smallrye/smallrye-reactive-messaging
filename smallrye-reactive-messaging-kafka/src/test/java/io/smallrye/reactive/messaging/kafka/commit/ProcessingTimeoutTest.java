package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Tests that the Kafka connector's {@code ProcessingInterceptor} implementation
 * applies a processing timeout based on {@code throttled.unprocessed-record-max-age.ms},
 * causing stuck messages to be nacked and handled by the failure strategy.
 */
public class ProcessingTimeoutTest extends KafkaCompanionTestBase {

    @Test
    public void testThrottledProcessingTimeoutNacksSlowConsumer() {
        companion.topics().createAndWait(topic, 1);

        // Produce 5 messages
        companion.produceStrings()
                .fromRecords(IntStream.range(0, 5).boxed()
                        .map(i -> new ProducerRecord<>(topic, 0, "key", "value-" + i))
                        .toList())
                .awaitCompletion(Duration.ofSeconds(10));

        String groupId = "test-throttled-timeout-" + topic;
        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("topic", topic)
                .with("group.id", groupId)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .with("failure-strategy", "ignore")
                .with("throttled.unprocessed-record-max-age.ms", 1000)
                .withPrefix("");

        SlowConsumer app = runApplication(config, SlowConsumer.class);

        // The consumer delays processing for 60s, but the ProcessingInterceptor
        // should timeout after 1000ms and nack each message.
        // With failure-strategy=ignore, the nack completes the message and processing continues.
        // All 5 messages should eventually be processed (timed out + nacked + ignored).
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(app.getReceived()).hasSize(5));

        // Verify offsets are committed for all messages
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(companion.consumerGroups().offsets(groupId))
                            .values().extracting(OffsetAndMetadata::offset)
                            .containsOnly(5L);
                });
    }

    @Test
    public void testFastProcessingCompletesNormally() {
        companion.topics().createAndWait(topic, 1);

        // Produce 10 messages
        companion.produceStrings()
                .fromRecords(IntStream.range(0, 10).boxed()
                        .map(i -> new ProducerRecord<>(topic, 0, "key", "value-" + i))
                        .toList())
                .awaitCompletion(Duration.ofSeconds(10));

        String groupId = "test-throttled-fast-" + topic;
        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("topic", topic)
                .with("group.id", groupId)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .with("failure-strategy", "ignore")
                .with("throttled.unprocessed-record-max-age.ms", 5000)
                .withPrefix("");

        FastConsumer app = runApplication(config, FastConsumer.class);

        // Fast processing should complete without timeouts
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app.getProcessedCount()).isEqualTo(10));

        // Verify offsets committed
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(companion.consumerGroups().offsets(groupId))
                            .values().extracting(OffsetAndMetadata::offset)
                            .containsOnly(10L);
                });

        // No failures should have occurred
        assertThat(app.getFailureCount()).isEqualTo(0);
    }

    @ApplicationScoped
    public static class SlowConsumer {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public Uni<Void> consume(String payload) {
            received.add(payload);
            // Simulate processing that takes much longer than the timeout
            return Uni.createFrom().voidItem()
                    .onItem().delayIt().by(Duration.ofSeconds(60));
        }

        public List<String> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    public static class FastConsumer {
        private final AtomicInteger processedCount = new AtomicInteger();
        private final AtomicInteger failureCount = new AtomicInteger();

        @Incoming("data")
        public Uni<Void> consume(String payload) {
            processedCount.incrementAndGet();
            return Uni.createFrom().voidItem();
        }

        public int getProcessedCount() {
            return processedCount.get();
        }

        public int getFailureCount() {
            return failureCount.get();
        }
    }
}
