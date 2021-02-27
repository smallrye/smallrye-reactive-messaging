package io.smallrye.reactive.messaging.kafka.serde;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.enterprise.inject.AmbiguousResolutionException;
import javax.enterprise.inject.UnsatisfiedResolutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.*;
import io.smallrye.reactive.messaging.kafka.fault.DeserializerWrapper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.JsonObjectDeserializer;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;

@SuppressWarnings("unchecked")
public class ValueDeserializerConfigurationTest extends KafkaTestBase {

    private KafkaSource<String, String> source;

    @AfterEach
    public void cleanup() {
        if (source != null) {
            source.closeQuietly();
        }
    }

    @Test
    public void testMissingValueDeserializerInConfig() {
        MapBasedConfig config = commonConsumerConfiguration()
                .without("value.deserializer");

        assertThatThrownBy(() -> {
            source = new KafkaSource<>(vertx, "my-group",
                    new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                    CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("value.deserializer");
    }

    @Test
    public void testValueDeserializationFailureWhenNoDeserializerSet() {
        MapBasedConfig config = commonConsumerConfiguration();
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage
                .produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new DoubleSerializer(),
                        null, () -> new ProducerRecord<>(topic, null, 12545.23));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<String, String> record = (KafkaRecord<String, String>) m;
            assertThat(record.getKey()).isNull();
            // Deserialization provides something non printable
            assertThat(record.getPayload()).isNotBlank();
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produceStrings(1, null, () -> new ProducerRecord<>(topic, "my-key", "hello-2"));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<String, String> record = (KafkaRecord<String, String>) list.get(1);
        assertThat(record.getKey()).isEqualTo("my-key");
        assertThat(record.getPayload()).isEqualTo("hello-2");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testValueDeserializationFailureWithDeserializerSet() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.deserializer", JsonObjectDeserializer.class.getName());
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage
                .produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new DoubleSerializer(),
                        null, () -> new ProducerRecord<>(topic, null, 6945231.56));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<String, JsonObject> record = (KafkaRecord<String, JsonObject>) m;
            assertThat(record.getKey()).isEqualTo(null);
            assertThat(record.getPayload()).isEqualTo(null);
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new JsonObjectSerializer(),
                null, () -> new ProducerRecord<>(topic, "hello-2", new JsonObject().put("k", "my-key")));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<String, JsonObject> record = (KafkaRecord<String, JsonObject>) list.get(1);
        assertThat(record.getPayload()).isEqualTo(new JsonObject().put("k", "my-key"));
        assertThat(record.getKey()).isEqualTo("hello-2");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testThatUnderlyingDeserializerReceiveTheConfiguration() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.deserializer", ConstantDeserializer.class.getName())
                .with("deserializer.value", "constant");
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage
                .produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new StringSerializer(),
                        null, () -> new ProducerRecord<>(topic, "key", "hello"));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<String, String> record = (KafkaRecord<String, String>) m;
            assertThat(record.getKey()).isEqualTo("key");
            assertThat(record.getPayload()).isEqualTo("constant");
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new JsonObjectSerializer(),
                null, () -> new ProducerRecord<>(topic, "key", new JsonObject().put("k", "my-key")));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<String, String> record = (KafkaRecord<String, String>) list.get(1);
        assertThat(record.getKey()).isEqualTo("key");
        assertThat(record.getPayload()).isEqualTo("constant");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testValueDeserializationFailureWithMatchingHandler() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.deserializer", JsonObjectDeserializer.class.getName())
                .with("value-deserialization-failure-handler", "my-deserialization-handler");

        JsonObject fallback = new JsonObject().put("fallback", "fallback");
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, new SingletonInstance<>("my-deserialization-handler",
                        new DeserializationFailureHandler<JsonObject>() {
                            @Override
                            public JsonObject handleDeserializationFailure(String topic, boolean isKey, String deserializer,
                                    byte[] data,
                                    Exception exception, Headers headers) {
                                assertThat(exception).isNotNull();
                                assertThat(ValueDeserializerConfigurationTest.this.topic).isEqualTo(topic);
                                assertThat(deserializer).isEqualTo(JsonObjectDeserializer.class.getName());
                                assertThat(data).isNotEmpty();
                                assertThat(isKey).isFalse();
                                assertThat(getHeader(headers, DeserializerWrapper.DESERIALIZATION_FAILURE_TOPIC))
                                        .isEqualTo(topic);
                                assertThat(getHeader(headers, DeserializerWrapper.DESERIALIZATION_FAILURE_REASON))
                                        .isEqualTo(exception.getMessage());
                                assertThat(headers.lastHeader(DeserializerWrapper.DESERIALIZATION_FAILURE_DATA).value())
                                        .isEqualTo(data);
                                assertThat(getHeader(headers, DeserializerWrapper.DESERIALIZATION_FAILURE_DESERIALIZER))
                                        .isEqualTo(deserializer);

                                return fallback;
                            }
                        }),
                -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage
                .produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new DoubleSerializer(),
                        null, () -> new ProducerRecord<>(topic, "hello", 698745231.56));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<String, JsonObject> record = (KafkaRecord<String, JsonObject>) m;
            assertThat(record.getPayload()).isEqualTo(fallback);
            assertThat(record.getKey()).isEqualTo("hello");
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new JsonObjectSerializer(),
                null, () -> new ProducerRecord<>(topic, "hello-2", new JsonObject().put("k", "my-key")));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<String, JsonObject> record = (KafkaRecord<String, JsonObject>) list.get(1);
        // Deserialization failure - no handler
        assertThat(record.getPayload()).isEqualTo(new JsonObject().put("k", "my-key"));
        assertThat(record.getKey()).isEqualTo("hello-2");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testWhenBothValueAndKeyFailureHandlerAreSetToTheSameHandler() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.deserializer", JsonObjectDeserializer.class.getName())
                .with("key.deserializer", JsonObjectDeserializer.class.getName())
                .with("value-deserialization-failure-handler", "my-deserialization-handler")
                .with("key-deserialization-failure-handler", "my-deserialization-handler");

        JsonObject fallbackForValue = new JsonObject().put("fallback", "fallback");
        JsonObject fallbackForKey = new JsonObject().put("fallback", "key");
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, new SingletonInstance<>("my-deserialization-handler",
                        new DeserializationFailureHandler<JsonObject>() {
                            @Override
                            public JsonObject handleDeserializationFailure(String topic, boolean isKey, String deserializer,
                                    byte[] data,
                                    Exception exception, Headers headers) {
                                return isKey ? fallbackForKey : fallbackForValue;
                            }
                        }),
                -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        // Fail for value
        JsonObject key = new JsonObject().put("key", "key");
        usage
                .produce(UUID.randomUUID().toString(), 1, new JsonObjectSerializer(), new DoubleSerializer(),
                        null, () -> new ProducerRecord<>(topic, key, 698745231.56));
        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<JsonObject, JsonObject> record = (KafkaRecord<JsonObject, JsonObject>) m;
            assertThat(record.getPayload()).isEqualTo(fallbackForValue);
            assertThat(record.getKey()).isEqualTo(key);
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        // Fail for key
        JsonObject value = new JsonObject().put("value", "value");
        usage
                .produce(UUID.randomUUID().toString(), 1, new DoubleSerializer(), new JsonObjectSerializer(),
                        null, () -> new ProducerRecord<>(topic, 698745231.56, value));
        await().until(() -> list.size() == 2);

        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<JsonObject, JsonObject> record = (KafkaRecord<JsonObject, JsonObject>) list.get(1);
        assertThat(record.getPayload()).isEqualTo(value);
        assertThat(record.getKey()).isEqualTo(fallbackForKey);
        assertThat(record.getPartition()).isEqualTo(0);
        record.ack().toCompletableFuture().join();

        // Everything ok
        usage.produce(UUID.randomUUID().toString(), 1, new JsonObjectSerializer(), new JsonObjectSerializer(),
                null, () -> new ProducerRecord<>(topic, key, value));

        await().until(() -> list.size() == 3);
        assertThat(list.get(2)).isInstanceOf(KafkaRecord.class);
        record = (KafkaRecord<JsonObject, JsonObject>) list.get(2);
        assertThat(record.getPayload()).isEqualTo(value);
        assertThat(record.getKey()).isEqualTo(key);
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(2).ack().toCompletableFuture().join();
    }

    @Test
    public void testValueDeserializationFailureWithMatchingHandlerReturningNull() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.deserializer", JsonObjectDeserializer.class.getName())
                .with("value-deserialization-failure-handler", "my-deserialization-handler");

        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, new SingletonInstance<>("my-deserialization-handler",
                        new DeserializationFailureHandler<JsonObject>() {
                            @Override
                            public JsonObject handleDeserializationFailure(String topic, boolean isKey, String deserializer,
                                    byte[] data,
                                    Exception exception, Headers headers) {
                                return null;
                            }
                        }),
                -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage
                .produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new DoubleSerializer(),
                        null, () -> new ProducerRecord<>(topic, "hello", 6987451.56));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<String, JsonObject> record = (KafkaRecord<String, JsonObject>) m;
            assertThat(record.getPayload()).isNull();
            assertThat(record.getKey()).isEqualTo("hello");
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produce(UUID.randomUUID().toString(), 1, new StringSerializer(), new JsonObjectSerializer(),
                null, () -> new ProducerRecord<>(topic, "hello-2", new JsonObject().put("k", "my-key")));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<String, JsonObject> record = (KafkaRecord<String, JsonObject>) list.get(1);
        // Deserialization failure - no handler
        assertThat(record.getPayload()).isEqualTo(new JsonObject().put("k", "my-key"));
        assertThat(record.getKey()).isEqualTo("hello-2");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testValueDeserializationFailureWithNoMatchingHandler() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.deserializer", JsonObjectDeserializer.class.getName())
                .with("value-deserialization-failure-handler", "my-deserialization-handler");

        assertThatThrownBy(() -> {
            source = new KafkaSource<>(vertx, "my-group",
                    new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                    CountKafkaCdiEvents.noCdiEvents, new SingletonInstance<>("not-matching",
                            new DeserializationFailureHandler<JsonObject>() {
                                @Override
                                public JsonObject handleDeserializationFailure(String topic, boolean isKey, String deserializer,
                                        byte[] data,
                                        Exception exception, Headers headers) {
                                    fail("Should not be called");
                                    return null;
                                }
                            }),
                    -1);
        }).isInstanceOf(UnsatisfiedResolutionException.class).hasMessageContaining("my-deserialization-handler");
    }

    @Test
    public void testKeyDeserializationFailureWithMultipleMatchingHandler() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.deserializer", JsonObjectDeserializer.class.getName())
                .with("value-deserialization-failure-handler", "my-deserialization-handler");

        DeserializationFailureHandler<JsonObject> i1 = new DeserializationFailureHandler<JsonObject>() {
            @Override
            public JsonObject handleDeserializationFailure(String topic, boolean isKey, String deserializer,
                    byte[] data,
                    Exception exception, Headers headers) {
                fail("Should not be called");
                return null;
            }
        };

        DeserializationFailureHandler<JsonObject> i2 = new DeserializationFailureHandler<JsonObject>() {
            @Override
            public JsonObject handleDeserializationFailure(String topic, boolean isKey, String deserializer,
                    byte[] data,
                    Exception exception, Headers headers) {
                fail("Should not be called");
                return null;
            }
        };
        assertThatThrownBy(() -> {
            source = new KafkaSource<>(vertx, "my-group",
                    new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                    CountKafkaCdiEvents.noCdiEvents, new DoubleInstance<>("my-deserialization-handler", i1, i2),
                    -1);
        }).isInstanceOf(AmbiguousResolutionException.class).hasMessageContaining("my-deserialization-handler");
    }

    @Test
    public void testWhenValueDeserializerFailsDuringConfig() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("value.deserializer", BrokenDeserializerFailingDuringConfig.class.getName());

        assertThatThrownBy(() -> source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1)).isInstanceOf(KafkaException.class)
                        .hasStackTraceContaining("boom");

    }

    private MapBasedConfig commonConsumerConfiguration() {
        return new KafkaMapBasedConfig()
                .with("bootstrap.servers", getBootstrapServers())
                .with("channel-name", "channel")
                .with("topic", topic)
                .with("auto.offset.reset", "earliest")
                .with("health-enabled", false)
                .with("graceful-shutdown", false)
                .with("value.deserializer", StringDeserializer.class.getName());
    }

    public static class BrokenDeserializerFailingDuringConfig implements Deserializer<String> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            throw new IllegalArgumentException("boom");
        }

        @Override
        public String deserialize(String topic, byte[] data) {
            return null;
        }
    }

    public static class ConstantDeserializer implements Deserializer<String> {

        private String value;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.value = (String) configs.get("deserializer.value");
        }

        @Override
        public String deserialize(String topic, byte[] data) {
            return value;
        }
    }

}
