package io.smallrye.reactive.messaging.kafka.client;

import static io.smallrye.reactive.messaging.kafka.base.MockKafkaUtils.injectMockConsumer;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.inject.Instance;
import javax.enterprise.util.TypeLiteral;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class PauseResumeTest extends WeldTestBase {

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
    void testPauseResumeWithRequests() {
        MapBasedConfig config = commonConfiguration()
                .with("client.id", UUID.randomUUID().toString());
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        AssertSubscriber<IncomingKafkaRecord<String, String>> subscriber = source.getStream()
                .subscribe().withSubscriber(AssertSubscriber.create(1));

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
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "k", "v1"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, "k", "v2"));
        });

        await().until(() -> subscriber.getItems().size() == 1);

        await().until(() -> !consumer.paused().isEmpty());

        subscriber.request(1);
        await().until(() -> subscriber.getItems().size() == 2);

        await().until(() -> !consumer.paused().isEmpty());

        subscriber.request(1);
        await().until(() -> subscriber.getItems().size() == 3);

        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3, "k", "v0"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 4, "k", "v1"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 5, "k", "v2"));
        });

        await().until(() -> !consumer.paused().isEmpty());

        subscriber.request(2);
        await().until(() -> subscriber.getItems().size() == 5);

    }

    @Test
    void testPauseResumeWithBlockingConsumption() {
        MapBasedConfig config = commonConfiguration()
                .with("client.id", UUID.randomUUID().toString());
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<String> items = new ArrayList<>();
        source.getStream()
                .onItem().transformToUniAndConcatenate(item -> Uni.createFrom().item(item)
                        .onItem().delayIt().by(Duration.ofMillis(1000))
                        .onItem().invoke(() -> items.add(item.getPayload())))
                .subscribe().with(item -> {
                    // do nothing
                });

        TopicPartition tp0 = new TopicPartition(TOPIC, 0);
        TopicPartition tp1 = new TopicPartition(TOPIC, 1);
        TopicPartition tp2 = new TopicPartition(TOPIC, 2);
        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(tp0, 0L);
        beginning.put(tp1, 0L);
        beginning.put(tp2, 0L);
        consumer.updateBeginningOffsets(beginning);

        // There is demand for the first item.
        await()
                .pollInterval(Duration.ofMillis(1))
                .until(() -> consumer.paused().isEmpty());

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Arrays.asList(tp0, tp1, tp2));
            for (int i = 0; i < 5; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k", "v" + i));
            }
        });

        await()
                .pollInterval(Duration.ofMillis(10))
                .until(() -> !consumer.paused().isEmpty());

        // It may await until everything is consumed, but the goal is to verify it resumed.
        await()
                .pollInterval(Duration.ofMillis(1))
                .until(() -> consumer.paused().isEmpty());
        await().until(() -> items.size() == 5);
    }

    @RepeatedTest(5)
    void testPauseResumeWithBlockingConsumptionAndConcurrency() {
        MapBasedConfig config = commonConfiguration()
                .with("client.id", UUID.randomUUID().toString());
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<String> items = new CopyOnWriteArrayList<>();
        source.getStream()
                .onItem().transformToUni(item -> Uni.createFrom().item(item)
                        .onItem().delayIt().by(Duration.ofMillis(1500))
                        .onItem().invoke(() -> items.add(item.getPayload())))
                .merge(2)
                .subscribe().with(item -> {
                    // do nothing
                });

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
            for (int i = 0; i < 5; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k", "v" + i));
            }
        });

        await().until(() -> items.size() > 3);
        await().until(() -> consumer.paused().isEmpty());
    }

    private MapBasedConfig commonConfiguration() {
        return new MapBasedConfig()
                .with("channel-name", "channel")
                .with("topic", TOPIC)
                .with("health-enabled", false)
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

}
