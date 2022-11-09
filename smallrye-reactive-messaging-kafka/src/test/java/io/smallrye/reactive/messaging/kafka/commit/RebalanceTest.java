package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.base.MockKafkaUtils.injectMockConsumer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

public class RebalanceTest extends WeldTestBase {

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

    @RepeatedTest(10)
    void testRebalance() throws InterruptedException {
        String group = UUID.randomUUID().toString();
        MapBasedConfig config = commonConfiguration()
                .with("lazy-client", true)
                .with("client.id", UUID.randomUUID().toString())
                .with("commit-strategy", "throttled")
                .with("auto.offset.reset", "earliest")
                .with("auto.commit.interval.ms", 100);
        source = new KafkaSource<>(vertx, group,
                new KafkaConnectorIncomingConfiguration(config),
                commitHandlerFactories, failureHandlerFactories,
                getConsumerRebalanceListeners(),
                CountKafkaCdiEvents.noCdiEvents, getDeserializationFailureHandlers(), -1);
        injectMockConsumer(source, consumer);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        TopicPartition p0 = new TopicPartition(TOPIC, 0);
        TopicPartition p1 = new TopicPartition(TOPIC, 1);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(p0, 0L);
        offsets.put(p1, 0L);
        consumer.updateBeginningOffsets(offsets);

        consumer.schedulePollTask(() -> {
            source.getCommitHandler().partitionsAssigned(offsets.keySet());
            consumer.rebalance(offsets.keySet());
            for (int i = 0; i < 500; i++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k", "v0-" + i));
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, i, "r", "v1-" + i));
            }
        });

        int expected = 500 * 2;
        await().until(() -> list.size() == expected);
        assertThat(list).hasSize(expected);

        consumer.schedulePollTask(() -> {
            source.getCommitHandler().partitionsRevoked(Collections.singleton(p1));
            source.getCommitHandler().partitionsAssigned(Collections.emptyList());
        });

        list.forEach(m -> {
            // Only commit one partition
            //noinspection deprecation
            if (m.getMetadata(IncomingKafkaRecordMetadata.class).map(IncomingKafkaRecordMetadata::getPartition)
                    .orElse(-1) == 0) {
                m.ack().toCompletableFuture().join();
            }
        });

        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(offsets.keySet());
            assertThat(committed.get(p0)).isNotNull();
            assertThat(committed.get(p0).offset()).isEqualTo(500);
            assertThat(committed.get(p1)).isNull();
        });

        // Continue inserting record in the wrong partition
        // This cannot happen in real world, but verify it does not fail
        consumer.schedulePollTask(() -> {
            for (int i = 501; i < 10000; i++) {
                IncomingKafkaRecord<String, String> r = new IncomingKafkaRecord<>(
                        new ConsumerRecord<>(TOPIC, 1, i, "r", "v1-" + i),
                        "channel", -1,
                        source.getCommitHandler(),
                        null, false, false);
                source.getCommitHandler().received(r).subscribeAsCompletionStage();
            }
        });

        Thread.sleep(100); // Wait the commit period

        HealthReport.HealthReportBuilder alive = HealthReport.builder();
        HealthReport.HealthReportBuilder ready = HealthReport.builder();
        source.isAlive(alive);
        source.isReady(ready);

        assertThat(alive.build().isOk()).isTrue();
        assertThat(ready.build().isOk()).isTrue();
    }

    private MapBasedConfig commonConfiguration() {
        return new MapBasedConfig()
                .with("channel-name", "channel")
                .with("graceful-shutdown", false)
                .with("topic", TOPIC)
                .with("health-enabled", false)
                .with("tracing-enabled", false)
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
