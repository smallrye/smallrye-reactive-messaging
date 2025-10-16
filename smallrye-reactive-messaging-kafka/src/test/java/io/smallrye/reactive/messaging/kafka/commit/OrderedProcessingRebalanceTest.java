package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.base.MockKafkaUtils.injectMockConsumer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class OrderedProcessingRebalanceTest extends WeldTestBase {

    private static final String TOPIC = "ordered-rebalance-topic";

    public Vertx vertx;
    private MockConsumer<String, String> consumer;
    private KafkaSource<String, String> source;

    @BeforeEach
    public void initializing() {
        vertx = Vertx.vertx();
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    }

    @AfterEach
    void closing() {
        if (source != null) {
            source.closeQuietly();
        }
        vertx.closeAndAwait();
    }

    @Test
    void testOrderedByKeyWithRebalance() throws InterruptedException {
        String group = UUID.randomUUID().toString();
        MapBasedConfig config = commonConfiguration()
                .with("lazy-client", true)
                .with("client.id", UUID.randomUUID().toString())
                .with("commit-strategy", "throttled")
                .with("throttled.ordered", "key")
                .with("auto.offset.reset", "earliest")
                .with("auto.commit.interval.ms", 100);

        source = new KafkaSource<>(vertx, group,
                new KafkaConnectorIncomingConfiguration(config),
                UnsatisfiedInstance.instance(), commitHandlerFactories, failureHandlerFactories,
                getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> receivedMessages = new CopyOnWriteArrayList<>();
        CountDownLatch processingLatch = new CountDownLatch(1);

        source.getStream()
                .onItem().transformToUniAndConcatenate(msg -> {
                    receivedMessages.add(msg);
                    // Simulate slow processing to keep messages in the pipeline during rebalance
                    return io.smallrye.mutiny.Uni.createFrom().item(msg)
                            .onItem().delayIt().by(Duration.ofMillis(50));
                })
                .subscribe().with(
                        msg -> msg.ack(),
                        failure -> {
                        },
                        processingLatch::countDown);

        TopicPartition p0 = new TopicPartition(TOPIC, 0);
        TopicPartition p1 = new TopicPartition(TOPIC, 1);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(p0, 0L);
        offsets.put(p1, 0L);
        consumer.updateBeginningOffsets(offsets);

        // Assign partitions and add initial records
        consumer.schedulePollTask(() -> {
            source.getCommitHandler().partitionsAssigned(offsets.keySet());
            consumer.rebalance(offsets.keySet());

            // Add records with different keys to partition 0
            for (int i = 0; i < 10; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "key-A", "value-p0-" + i));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i + 10, "key-B", "value-p0-" + i));
            }

            // Add records to partition 1
            for (int i = 0; i < 10; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, i, "key-C", "value-p1-" + i));
            }
        });

        // Wait for some messages to be received
        await().atMost(Duration.ofSeconds(5))
                .until(() -> receivedMessages.size() >= 15);

        int messageCountBeforeRebalance = receivedMessages.size();

        // Trigger rebalance - revoke partition 1
        consumer.schedulePollTask(() -> {
            source.getCommitHandler().partitionsRevoked(Collections.singleton(p1));
            consumer.rebalance(Collections.singleton(p0));
            source.getCommitHandler().partitionsAssigned(Collections.emptyList());
        });

        // Wait a bit for rebalance to be processed
        Thread.sleep(200);

        // Add more records to partition 0 after rebalance
        consumer.schedulePollTask(() -> {
            for (int i = 0; i < 5; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 20 + i, "key-A", "value-after-rebalance-" + i));
            }
        });

        // Wait for new messages to be processed
        await().atMost(Duration.ofSeconds(5))
                .until(() -> receivedMessages.size() >= messageCountBeforeRebalance + 3);

        // Verify that:
        // 1. Messages from partition 0 continue to be processed after rebalance
        // 2. Messages from partition 1 are minimal (only those already in pipeline)
        long p0MessagesAfterRebalance = receivedMessages.stream()
                .skip(messageCountBeforeRebalance)
                .filter(msg -> msg.getMetadata(IncomingKafkaRecordMetadata.class)
                        .map(m -> m.getPartition() == 0)
                        .orElse(false))
                .count();

        assertThat(p0MessagesAfterRebalance).isGreaterThan(0);

        long p1MessagesAfterRebalance = receivedMessages.stream()
                .skip(messageCountBeforeRebalance)
                .filter(msg -> msg.getMetadata(IncomingKafkaRecordMetadata.class)
                        .map(m -> m.getPartition() == 1)
                        .orElse(false))
                .count();

        // Allow a few messages from revoked partition that were already in the pipeline
        // but there should be significantly fewer than the messages we're actively processing
        assertThat(p1MessagesAfterRebalance).isLessThan(p0MessagesAfterRebalance);
    }

    @Test
    void testOrderedByPartitionWithRebalance() throws InterruptedException {
        String group = UUID.randomUUID().toString();
        MapBasedConfig config = commonConfiguration()
                .with("lazy-client", true)
                .with("client.id", UUID.randomUUID().toString())
                .with("commit-strategy", "throttled")
                .with("throttled.ordered", "partition")
                .with("auto.offset.reset", "earliest")
                .with("auto.commit.interval.ms", 100);

        source = new KafkaSource<>(vertx, group,
                new KafkaConnectorIncomingConfiguration(config),
                UnsatisfiedInstance.instance(), commitHandlerFactories, failureHandlerFactories,
                getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> receivedMessages = new CopyOnWriteArrayList<>();

        source.getStream()
                .onItem().transformToUniAndConcatenate(msg -> {
                    receivedMessages.add(msg);
                    return io.smallrye.mutiny.Uni.createFrom().item(msg)
                            .onItem().delayIt().by(Duration.ofMillis(50));
                })
                .subscribe().with(msg -> msg.ack());

        TopicPartition p0 = new TopicPartition(TOPIC, 0);
        TopicPartition p1 = new TopicPartition(TOPIC, 1);
        TopicPartition p2 = new TopicPartition(TOPIC, 2);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(p0, 0L);
        offsets.put(p1, 0L);
        offsets.put(p2, 0L);
        consumer.updateBeginningOffsets(offsets);

        // Assign all three partitions
        consumer.schedulePollTask(() -> {
            source.getCommitHandler().partitionsAssigned(offsets.keySet());
            consumer.rebalance(offsets.keySet());

            for (int i = 0; i < 5; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "key", "p0-" + i));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, i, "key", "p1-" + i));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 2, i, "key", "p2-" + i));
            }
        });

        // Wait for messages
        await().atMost(Duration.ofSeconds(5))
                .until(() -> receivedMessages.size() >= 10);

        // Revoke partition 1 and 2
        consumer.schedulePollTask(() -> {
            source.getCommitHandler().partitionsRevoked(Arrays.asList(p1, p2));
            consumer.rebalance(Collections.singleton(p0));
            source.getCommitHandler().partitionsAssigned(Collections.emptyList());
        });

        Thread.sleep(200);

        int beforeSize = receivedMessages.size();

        // Add more records only to partition 0
        consumer.schedulePollTask(() -> {
            for (int i = 5; i < 10; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "key", "p0-after-" + i));
            }
        });

        // Wait for new messages
        await().atMost(Duration.ofSeconds(5))
                .until(() -> receivedMessages.size() >= beforeSize + 3);

        // Verify only partition 0 messages received after rebalance
        long p0After = receivedMessages.stream().skip(beforeSize)
                .filter(msg -> msg.getMetadata(IncomingKafkaRecordMetadata.class)
                        .map(m -> m.getPartition() == 0).orElse(false))
                .count();

        assertThat(p0After).isGreaterThan(0);

        long p1After = receivedMessages.stream().skip(beforeSize)
                .filter(msg -> msg.getMetadata(IncomingKafkaRecordMetadata.class)
                        .map(m -> m.getPartition() == 1).orElse(false))
                .count();

        assertThat(p1After).isEqualTo(0);
    }

    private MapBasedConfig commonConfiguration() {
        return new MapBasedConfig()
                .with("channel-name", "channel")
                .with("graceful-shutdown", false)
                .with("topic", TOPIC)
                .with("health-enabled", false)
                .with("tracing-enabled", false)
                .with("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return getBeanManager().createInstance().select(KafkaConsumerRebalanceListener.class);
    }

    public Instance<DeserializationFailureHandler<?>> getDeserializationFailureHandlers() {
        return getBeanManager().createInstance().select(
                new TypeLiteral<DeserializationFailureHandler<?>>() {
                });
    }
}
