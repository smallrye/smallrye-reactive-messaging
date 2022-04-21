package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.base.MockKafkaUtils.injectMockConsumer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class BatchCommitStrategiesTest extends WeldTestBase {

    private static final String TOPIC = "my-topic";

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
    void testLatestCommitStrategy() {
        MapBasedConfig config = commonConfiguration()
                .with("commit-strategy", "latest")
                .with("client.id", UUID.randomUUID().toString());
        String group = UUID.randomUUID().toString();
        source = new KafkaSource<>(vertx, group,
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> list = new ArrayList<>();
        source.getBatchStream().subscribe().with(list::add);

        TopicPartition tp0 = new TopicPartition(TOPIC, 0);
        TopicPartition tp1 = new TopicPartition(TOPIC, 1);
        TopicPartition tp2 = new TopicPartition(TOPIC, 2);
        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(tp0, 0L);
        beginning.put(tp1, 0L);
        beginning.put(tp2, 0L);
        consumer.updateBeginningOffsets(beginning);

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Arrays.asList(tp0, tp1, tp2));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "k", "v0"));
        });

        await().until(() -> list.size() == 1);
        assertThat(list).hasSize(1);

        list.get(0).ack().toCompletableFuture().join();

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Collections.singleton(tp0));
            assertThat(committed.get(tp0).offset()).isEqualTo(1);
        });

        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "k", "v1"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, "k", "v2"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3, "k", "v3"));
        });

        await().until(() -> list.size() == 2);

        Message<?> message = list.get(1);
        message.ack().toCompletableFuture().join();
        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Collections.singleton(tp0));
            assertThat(committed.get(tp0).offset()).isEqualTo(4);
        });

        // latest commit strategy, 3 is not acked, but offset 4 got committed.

        // Do not change anything.
        list.get(1).ack().toCompletableFuture().join();
        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Collections.singleton(tp0));
            assertThat(committed.get(tp0).offset()).isEqualTo(4);
        });

        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, "k", "v4"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 2, 0, "k", "v5"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 1, "k", "v6"));
        });

        await().until(() -> list.size() == 3);

        list.get(2).ack().toCompletableFuture().join();
        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(Arrays.asList(tp0, tp1, tp2)));
            assertThat(committed.get(tp0).offset()).isEqualTo(4);
            assertThat(committed.get(tp1).offset()).isEqualTo(2);
            assertThat(committed.get(tp2).offset()).isEqualTo(1);
        });
    }

    @Test
    void testThrottledStrategy() {
        MapBasedConfig config = commonConfiguration()
                .with("commit-strategy", "throttled")
                .with("auto.commit.interval.ms", 100);
        String group = UUID.randomUUID().toString();
        source = new KafkaSource<>(vertx, group,
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> list = new ArrayList<>();
        source.getBatchStream()
                .subscribe().with(list::add);

        TopicPartition tp = new TopicPartition(TOPIC, 0);
        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(tp));
            source.getCommitHandler().partitionsAssigned(Collections.singletonList(tp));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "k", "v0"));
        });

        await().until(() -> list.size() == 1);
        assertThat(list).hasSize(1);

        list.get(0).ack().toCompletableFuture().join();

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Collections.singleton(tp));
            assertThat(committed.get(tp)).isNotNull();
            assertThat(committed.get(tp).offset()).isEqualTo(1);
        });

        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "k", "v1"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, "k", "v2"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3, "k", "v3"));
        });

        await().until(() -> list.size() == 2);

        list.get(1).ack().toCompletableFuture().join();

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Collections.singleton(tp));
            assertThat(committed.get(tp)).isNotNull();
            assertThat(committed.get(tp).offset()).isEqualTo(4);
        });

    }

    @RepeatedTest(10)
    void testThrottledStrategyWithManyRecords() {
        MapBasedConfig config = commonConfiguration()
                .with("client.id", UUID.randomUUID().toString())
                .with("commit-strategy", "throttled")
                .with("auto.offset.reset", "earliest")
                .with("auto.commit.interval.ms", 100);
        String group = UUID.randomUUID().toString();
        source = new KafkaSource<>(vertx, group,
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> list = new CopyOnWriteArrayList<>();
        source.getBatchStream()
                .subscribe().with(list::add);

        TopicPartition p0 = new TopicPartition(TOPIC, 0);
        TopicPartition p1 = new TopicPartition(TOPIC, 1);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(p0, 0L);
        offsets.put(p1, 5L);
        consumer.updateBeginningOffsets(offsets);

        consumer.schedulePollTask(() -> {
            consumer.rebalance(offsets.keySet());
            source.getCommitHandler().partitionsAssigned(offsets.keySet());
        });
        for (int i = 0; i < 500; i++) {
            int j = i;
            consumer.schedulePollTask(() -> {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, j, "k", "v0-" + j));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, j, "r", "v1-" + j));
            });
        }

        // Expected number of messages: 500 messages in each partition minus the [0..5) messages from p1
        int expected = 500 * 2 - 5;
        await().until(() -> list.stream().map(IncomingKafkaRecordBatch.class::cast)
                .mapToLong(r -> r.getRecords().size()).sum() == expected);

        list.forEach(m -> m.ack().toCompletableFuture().join());

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(offsets.keySet());
            assertThat(committed.get(p0)).isNotNull();
            assertThat(committed.get(p0).offset()).isEqualTo(500);
            assertThat(committed.get(p1)).isNotNull();
            assertThat(committed.get(p1).offset()).isEqualTo(500);
        });

        for (int i = 0; i < 1000; i++) {
            int j = i;
            consumer.schedulePollTask(() -> {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 500 + j, "k", "v0-" + (500 + j)));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 500 + j, "k", "v1-" + (500 + j)));
            });
        }

        int expected2 = expected + 1000 * 2;
        await().until(() -> list.stream().map(IncomingKafkaRecordBatch.class::cast)
                .mapToLong(r -> r.getRecords().size()).sum() == expected2);

        list.forEach(m -> m.ack().toCompletableFuture().join());

        await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(offsets.keySet());
                    assertThat(committed.get(p0)).isNotNull();
                    assertThat(committed.get(p0).offset()).isEqualTo(1500);
                    assertThat(committed.get(p1)).isNotNull();
                    assertThat(committed.get(p1).offset()).isEqualTo(1500);
                });

        @SuppressWarnings("unchecked")
        List<String> payloads = list.stream().map(m -> (List<String>) m.getPayload())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        for (int i = 0; i < 1500; i++) {
            assertThat(payloads).contains("v0-" + i);
        }
        for (int i = 5; i < 1500; i++) {
            assertThat(payloads).contains("v1-" + i);
        }
    }

    @Test
    void testThrottledStrategyWithTooManyUnackedMessages() {
        MapBasedConfig config = commonConfiguration()
                .with("client.id", UUID.randomUUID().toString())
                .with("commit-strategy", "throttled")
                .with("auto.offset.reset", "earliest")
                .with("health-enabled", true)
                .with("throttled.unprocessed-record-max-age.ms", 1000)
                .with("auto.commit.interval.ms", 100);
        String group = UUID.randomUUID().toString();
        source = new KafkaSource<>(vertx, group,
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> list = new CopyOnWriteArrayList<>();
        source.getBatchStream()
                .subscribe().with(list::add);

        TopicPartition p0 = new TopicPartition(TOPIC, 0);
        TopicPartition p1 = new TopicPartition(TOPIC, 1);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(p0, 0L);
        offsets.put(p1, 5L);
        consumer.updateBeginningOffsets(offsets);

        consumer.schedulePollTask(() -> {
            consumer.rebalance(offsets.keySet());
            source.getCommitHandler().partitionsAssigned(offsets.keySet());
        });
        for (int i = 0; i < 500; i++) {
            int j = i;
            consumer.schedulePollTask(() -> {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, j, "k", "v0-" + j));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, j, "r", "v1-" + j));
            });
        }

        // Expected number of messages: 500 messages in each partition minus the [0..5) messages from p1
        int expected = 500 * 2 - 5;
        await().until(() -> list.stream().map(IncomingKafkaRecordBatch.class::cast)
                .mapToLong(r -> r.getRecords().size()).sum() == expected);

        // Ack first 10 records
        int count = 0;
        for (Message<?> message : list) {
            assertThat(message.getMetadata(IncomingKafkaRecordBatchMetadata.class)).isPresent();
            if (count < 10) {
                message.ack().toCompletableFuture().join();
                count++;
            }
        }

        // wait until health check is not ok
        await().until(() -> {
            HealthReport.HealthReportBuilder builder = HealthReport.builder();
            source.isAlive(builder);
            HealthReport r = builder.build();
            return !r.isOk();
        });

        // build the health check again and get the report message
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        source.isAlive(builder);
        String message = builder.build().getChannels().get(0).getMessage();
        assertThat(message).containsAnyOf("my-topic-0", "my-topic-1");
    }

    @Test
    public void testWithRebalanceListenerMatchGivenName() {
        addBeans(NamedRebalanceListener.class);
        MapBasedConfig config = commonConfiguration();
        config
                .with("consumer-rebalance-listener.name", "mine")
                .with("client.id", UUID.randomUUID().toString());
        String group = UUID.randomUUID().toString();
        source = new KafkaSource<>(vertx, group,
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);

        injectMockConsumer(source, consumer);

        List<Message<?>> list = new ArrayList<>();
        source.getBatchStream()
                .subscribe().with(list::add);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TOPIC, 0), 0L);
        offsets.put(new TopicPartition(TOPIC, 1), 0L);
        consumer.updateBeginningOffsets(offsets);

        consumer.schedulePollTask(() -> {
            TopicPartition tp = new TopicPartition(TOPIC, 0);
            consumer.rebalance(Collections.singletonList(tp));
            source.getCommitHandler().partitionsAssigned(Collections.singletonList(tp));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "k", "v"));
        });

        await().until(() -> list.size() == 1);
        assertThat(list).hasSize(1);

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 1)));
            ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 1, 0, "k", "v");
            consumer.addRecord(record);
        });

        await().until(() -> list.size() == 2);
        assertThat(list).hasSize(2);
    }

    private MapBasedConfig commonConfiguration() {
        return new MapBasedConfig()
                .with("channel-name", "channel")
                .with("graceful-shutdown", false)
                .with("topic", TOPIC)
                .with("health-enabled", false)
                .with("tracing-enabled", false)
                .with("batch", true)
                .with("value.deserializer", StringDeserializer.class.getName());
    }

    public Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return getBeanManager().createInstance().select(KafkaConsumerRebalanceListener.class);
    }

    public Instance<DeserializationFailureHandler<?>> getDeserializationFailureHandlers() {
        return getBeanManager().createInstance().select(
                new TypeLiteral<DeserializationFailureHandler<?>>() {
                });
    }

    @ApplicationScoped
    @Identifier("mine")
    public static class NamedRebalanceListener implements KafkaConsumerRebalanceListener {

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer,
                Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer,
                Collection<TopicPartition> partitions) {

        }

    }

    @SuppressWarnings("unused")
    @ApplicationScoped
    @Identifier("mine")
    public static class SameNameRebalanceListener extends NamedRebalanceListener
            implements KafkaConsumerRebalanceListener {

    }

}
