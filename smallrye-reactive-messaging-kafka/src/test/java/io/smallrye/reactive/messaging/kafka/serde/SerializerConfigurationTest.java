package io.smallrye.reactive.messaging.kafka.serde;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.*;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.kafka.*;
import io.smallrye.reactive.messaging.kafka.base.*;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;

@SuppressWarnings("unchecked")
public class SerializerConfigurationTest extends KafkaTestBase {

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
        sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents);

        List<Tuple2<String, String>> list = new ArrayList<>();
        usage.consumeStrings(topic, 4, 10, TimeUnit.SECONDS, () -> {
        }, (k, v) -> {
            list.add(Tuple2.of(k, v));
        });

        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        Multi.createFrom().items(
                Message.of(Record.of("key", "value")), Message.of(Record.of(null, "value")),
                Message.of(Record.of("key", null)), Message.of(Record.of(null, null)))
                .subscribe((Subscriber<? super Message<?>>) subscriber);

        await().until(() -> list.size() == 4);
        assertThat(list.get(0).getItem1()).isEqualTo("key");
        assertThat(list.get(0).getItem2()).isEqualTo("value");

        assertThat(list.get(1).getItem1()).isEqualTo(null);
        assertThat(list.get(1).getItem2()).isEqualTo("value");

        assertThat(list.get(2).getItem1()).isEqualTo("key");
        assertThat(list.get(2).getItem2()).isEqualTo(null);

        assertThat(list.get(3).getItem1()).isEqualTo(null);
        assertThat(list.get(3).getItem2()).isEqualTo(null);

    }

    @Test
    public void testKeySerializationFailure() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.serializer", JsonObjectSerializer.class.getName())
                .with("key.serializer", JsonObjectSerializer.class.getName())
                .with("retries", 0L);
        sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents);
        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        AtomicBoolean nacked = new AtomicBoolean();
        Multi.createFrom().items(
                Message.of(Record.of(125.25, new JsonObject().put("k", "v"))).withNack(t -> {
                    nacked.set(true);
                    return CompletableFuture.completedFuture(null);
                })).subscribe((Subscriber<? super Message<?>>) subscriber);

        await().until(nacked::get);
    }

    @Test
    public void testValueSerializationFailure() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.serializer", JsonObjectSerializer.class.getName())
                .with("key.serializer", JsonObjectSerializer.class.getName())
                .with("retries", 0L);
        sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents);
        Subscriber<? extends Message<?>> subscriber = sink.getSink().build();
        AtomicBoolean nacked = new AtomicBoolean();
        Multi.createFrom().items(
                Message.of(Record.of(new JsonObject().put("k", "v"), 125.25)).withNack(t -> {
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
            sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("value.serializer");

    }

    @Test
    public void testFailureWhenSerializerFailsDuringConfiguration() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.serializer", BrokenSerializerFailingDuringConfig.class.getName());

        assertThatThrownBy(() -> {
            sink = new KafkaSink(new KafkaConnectorOutgoingConfiguration(config), CountKafkaCdiEvents.noCdiEvents);
        }).isInstanceOf(KafkaException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("boom");

    }

    private MapBasedConfig commonConsumerConfiguration() {
        return new MapBasedConfig()
                .with("bootstrap.servers", getBootstrapServers())
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
