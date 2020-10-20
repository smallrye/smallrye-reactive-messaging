package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.UnsatisfiedResolutionException;
import javax.enterprise.inject.spi.DeploymentException;
import javax.inject.Named;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

public class CommitStrategiesTest extends WeldTestBase {

    private static final String TOPIC = "my-topic";

    public Vertx vertx;
    private MockConsumer<String, String> consumer;

    @BeforeEach
    public void initializing() {
        vertx = Vertx.vertx();
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    }

    @AfterEach
    void closing() {
        vertx.closeAndAwait();
    }

    @Test
    void testLatestCommitStrategy() {
        MapBasedConfig config = commonConfiguration().with("commit-strategy", "latest").with("client.id",
                UUID.randomUUID().toString());
        KafkaSource<String, String> source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

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

        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Collections.singleton(tp0));
        assertThat(committed.get(tp0).offset()).isEqualTo(1);

        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "k", "v1"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, "k", "v2"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3, "k", "v3"));
        });

        await().until(() -> list.size() == 4);

        Message<?> message = list.get(1);
        message.ack().toCompletableFuture().join();
        committed = consumer.committed(Collections.singleton(tp0));
        assertThat(committed.get(tp0).offset()).isEqualTo(2);

        // latest commit strategy, 3 is not acked, but offset 4 got committed.

        list.get(3).ack().toCompletableFuture().join();
        committed = consumer.committed(Collections.singleton(tp0));
        assertThat(committed.get(tp0).offset()).isEqualTo(4);

        // Do not change anything.
        list.get(2).ack().toCompletableFuture().join();
        committed = consumer.committed(Collections.singleton(tp0));
        assertThat(committed.get(tp0).offset()).isEqualTo(4);

        // Do not change anything.
        list.get(1).ack().toCompletableFuture().join();
        committed = consumer.committed(Collections.singleton(tp0));
        assertThat(committed.get(tp0).offset()).isEqualTo(4);

        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, "k", "v4"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 2, 0, "k", "v5"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 1, "k", "v6"));
        });

        await().until(() -> list.size() == 7);

        Message<?> v6 = list.stream().filter(m -> m.getPayload().equals("v6")).findFirst().orElse(null);
        assertThat(v6).isNotNull();
        v6.ack().toCompletableFuture().join();
        committed = consumer.committed(new HashSet<>(Arrays.asList(tp0, tp1, tp2)));
        assertThat(committed.get(tp0).offset()).isEqualTo(4);
        assertThat(committed.get(tp1).offset()).isEqualTo(2);
        assertThat(committed.get(tp2)).isNull();

        Message<?> v5 = list.stream().filter(m -> m.getPayload().equals("v5")).findFirst().orElse(null);
        assertThat(v5).isNotNull();
        v5.ack().toCompletableFuture().join();
        committed = consumer.committed(new HashSet<>(Arrays.asList(tp0, tp1, tp2)));
        assertThat(committed.get(tp0).offset()).isEqualTo(4);
        assertThat(committed.get(tp1).offset()).isEqualTo(2);
        assertThat(committed.get(tp2).offset()).isEqualTo(1);
    }

    @Test
    void testThrottledStrategy() {
        MapBasedConfig config = commonConfiguration()
                .with("commit-strategy", "throttled")
                .with("auto.commit.interval.ms", 100);
        KafkaSource<String, String> source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        TopicPartition tp = new TopicPartition(TOPIC, 0);
        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
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

        await().until(() -> list.size() == 4);

        list.get(2).ack().toCompletableFuture().join();
        list.get(1).ack().toCompletableFuture().join();

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Collections.singleton(tp));
            assertThat(committed.get(tp)).isNotNull();
            assertThat(committed.get(tp).offset()).isEqualTo(3);
        });

        list.get(3).ack().toCompletableFuture().join();

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Collections.singleton(tp));
            assertThat(committed.get(tp)).isNotNull();
            assertThat(committed.get(tp).offset()).isEqualTo(4);
        });

    }

    @Test
    void testThrottledStrategyWithManyRecords() {
        MapBasedConfig config = commonConfiguration()
                .with("client.id", UUID.randomUUID().toString())
                .with("commit-strategy", "throttled")
                .with("auto.offset.reset", "earliest")
                .with("auto.commit.interval.ms", 100);
        KafkaSource<String, String> source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        TopicPartition p0 = new TopicPartition(TOPIC, 0);
        TopicPartition p1 = new TopicPartition(TOPIC, 1);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(p0, 0L);
        offsets.put(p1, 5L);
        consumer.updateBeginningOffsets(offsets);

        consumer.schedulePollTask(() -> {
            consumer.rebalance(offsets.keySet());
            for (int i = 0; i < 500; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k", "v0-" + i));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, i, "r", "v1-" + i));
            }
        });

        // Expected number of messages: 500 messages in each partition minus the [0..5) messages from p1
        int expected = 500 * 2 - 5;
        await().until(() -> list.size() == expected);
        assertThat(list).hasSize(expected);

        list.forEach(m -> m.ack().toCompletableFuture().join());

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(offsets.keySet());
            assertThat(committed.get(p0)).isNotNull();
            assertThat(committed.get(p0).offset()).isEqualTo(500);
            assertThat(committed.get(p1)).isNotNull();
            assertThat(committed.get(p1).offset()).isEqualTo(500);
        });

        consumer.schedulePollTask(() -> {
            for (int i = 0; i < 1000; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 500 + i, "k", "v0-" + (500 + i)));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 500 + i, "k", "v1-" + (500 + i)));
            }
        });

        int expected2 = expected + 1000 * 2;
        await().until(() -> list.size() == expected2);

        list.forEach(m -> m.ack().toCompletableFuture().join());

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(offsets.keySet());
            assertThat(committed.get(p0)).isNotNull();
            assertThat(committed.get(p0).offset()).isEqualTo(1500);
            assertThat(committed.get(p1)).isNotNull();
            assertThat(committed.get(p1).offset()).isEqualTo(1500);
        });

        List<String> payloads = list.stream().map(m -> (String) m.getPayload()).collect(Collectors.toList());
        for (int i = 0; i < 1500; i++) {
            assertThat(payloads).contains("v0-" + i);
        }
        for (int i = 5; i < 1500; i++) {
            assertThat(payloads).contains("v1-" + i);
        }
    }

    @Test
    public void testFailureWhenNoRebalanceListenerMatchGivenName() {
        MapBasedConfig config = commonConfiguration();
        config
                .with("client.id", UUID.randomUUID().toString())
                .with("consumer-rebalance-listener.name", "my-missing-name");
        assertThatThrownBy(() -> {
            new KafkaSource<>(vertx, "my-group",
                    new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                    CountKafkaCdiEvents.noCdiEvents, -1);
        }).isInstanceOf(UnsatisfiedResolutionException.class);
    }

    @Test
    public void testFailureWhenMultipleRebalanceListenerMatchGivenName() {
        MapBasedConfig config = commonConfiguration();
        addBeans(NamedRebalanceListener.class, SameNameRebalanceListener.class);
        config
                .with("consumer-rebalance-listener.name", "mine")
                .with("client.id", UUID.randomUUID().toString());
        assertThatThrownBy(() -> new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, -1)).isInstanceOf(DeploymentException.class).hasMessageContaining("mine");
    }

    @Test
    public void testWithRebalanceListenerMatchGivenName() {
        addBeans(NamedRebalanceListener.class);
        MapBasedConfig config = commonConfiguration();
        config
                .with("consumer-rebalance-listener.name", "mine")
                .with("client.id", UUID.randomUUID().toString());
        KafkaSource<String, String> source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, -1);

        injectMockConsumer(source, consumer);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TOPIC, 0), 0L);
        offsets.put(new TopicPartition(TOPIC, 1), 0L);
        consumer.updateBeginningOffsets(offsets);

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
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
                .with("topic", TOPIC)
                .with("health-enabled", false)
                .with("value.deserializer", StringDeserializer.class.getName());
    }

    @SuppressWarnings("rawtypes")
    private void injectMockConsumer(KafkaSource<String, String> source, MockConsumer<String, String> consumer) {
        try {
            KafkaConsumer<String, String> cons = source.getConsumer();
            KafkaReadStream stream = cons.getDelegate().asStream();
            Field field = stream.getClass().getDeclaredField("consumer");
            field.setAccessible(true);
            field.set(stream, consumer);
            // Close the initial consumer.
            cons.closeAndAwait();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to inject mock consumer", e);
        }
    }

    public Instance<KafkaConsumerRebalanceListener> getConsumerRebalanceListeners() {
        return getBeanManager().createInstance().select(KafkaConsumerRebalanceListener.class);
    }

    @ApplicationScoped
    @Named("mine")
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

    @ApplicationScoped
    @Named("mine")
    public static class SameNameRebalanceListener extends NamedRebalanceListener
            implements KafkaConsumerRebalanceListener {

    }

}
