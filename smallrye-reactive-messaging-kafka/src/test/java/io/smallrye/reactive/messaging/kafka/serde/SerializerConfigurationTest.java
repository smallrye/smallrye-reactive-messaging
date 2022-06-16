package io.smallrye.reactive.messaging.kafka.serde;

import static io.smallrye.reactive.messaging.kafka.Record.of;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.base.JsonObjectSerde;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;

@SuppressWarnings("unchecked")
public class SerializerConfigurationTest extends KafkaCompanionTestBase {

    private KafkaSink sink;

    @AfterEach
    public void cleanup() {
        if (sink != null) {
            sink.closeQuietly();
        }
    }

    @Test
    public void testThatWhenNotSetKeySerializerIsString() {
        MapBasedConfig config = commonConsumerConfiguration();
        sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance());

        ConsumerTask<String, String> consumed = companion.consumeStrings().fromTopics(topic, 4, Duration.ofSeconds(10));

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Multi.createFrom().items(
                Message.of(of("key", "value")), Message.of(of(null, "value")),
                Message.of(of("key", null)), Message.of(of(null, null)))
                .subscribe((Subscriber<? super Message<?>>) subscriber);

        await().until(() -> consumed.getRecords().size() == 4);
        assertThat(consumed.getRecords().get(0).key()).isEqualTo("key");
        assertThat(consumed.getRecords().get(0).value()).isEqualTo("value");

        assertThat(consumed.getRecords().get(1).key()).isEqualTo(null);
        assertThat(consumed.getRecords().get(1).value()).isEqualTo("value");

        assertThat(consumed.getRecords().get(2).key()).isEqualTo("key");
        assertThat(consumed.getRecords().get(2).value()).isEqualTo(null);

        assertThat(consumed.getRecords().get(3).key()).isEqualTo(null);
        assertThat(consumed.getRecords().get(3).value()).isEqualTo(null);

    }

    @Test
    public void testKeySerializationFailure() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.serializer", JsonObjectSerde.JsonObjectSerializer.class.getName())
                .with("key.serializer", JsonObjectSerde.JsonObjectSerializer.class.getName())
                .with("retries", 0L);
        sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance());
        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        AtomicBoolean nacked = new AtomicBoolean();
        Multi.createFrom().items(
                Message.of(of(125.25, new JsonObject().put("k", "v"))).withNack(t -> {
                    nacked.set(true);
                    return CompletableFuture.completedFuture(null);
                })).subscribe((Subscriber<? super Message<?>>) subscriber);

        await().until(nacked::get);
    }

    @Test
    public void testValueSerializationFailure() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.serializer", JsonObjectSerde.JsonObjectSerializer.class.getName())
                .with("key.serializer", JsonObjectSerde.JsonObjectSerializer.class.getName())
                .with("retries", 0L);
        sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance());
        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        AtomicBoolean nacked = new AtomicBoolean();
        Multi.createFrom().items(
                Message.of(of(new JsonObject().put("k", "v"), 125.25)).withNack(t -> {
                    nacked.set(true);
                    return CompletableFuture.completedFuture(null);
                })).subscribe((Subscriber<? super Message<?>>) subscriber);

        await().until(nacked::get);
    }

    @Test
    public void testFailureWhenValueSerializerIsNotSet() {
        MapBasedConfig config = commonConsumerConfiguration()
                .without("value.serializer");

        assertThatThrownBy(() -> {
            sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents,
                    UnsatisfiedInstance.instance());
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("value.serializer");

    }

    @Test
    public void testFailureWhenSerializerFailsDuringConfiguration() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.serializer", BrokenSerializerFailingDuringConfig.class.getName());

        assertThatThrownBy(() -> {
            sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents,
                    UnsatisfiedInstance.instance());
        }).isInstanceOf(KafkaException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("boom");

    }

    private MapBasedConfig commonConsumerConfiguration() {
        return new MapBasedConfig()
                .with("bootstrap.servers", companion.getBootstrapServers())
                .with("channel-name", "channel")
                .with("topic", topic)
                .with("health-enabled", false)
                .with("tracing-enabled", false)
                .with("value.serializer", StringSerializer.class.getName());
    }

    public static class BrokenSerializerFailingDuringConfig implements Serializer<String> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            throw new IllegalStateException("boom");
        }

        @Override
        public byte[] serialize(String topic, String data) {
            return fail("Should not be called");
        }
    }

}
