package io.smallrye.reactive.messaging.kafka.serde;

import static org.assertj.core.api.Assertions.*;
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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
public class KeyDeserializerConfigurationTest extends KafkaTestBase {

    private KafkaSource<String, String> source;

    @AfterEach
    public void cleanup() {
        if (source != null) {
            source.closeQuietly();
        }
    }

    @Test
    public void testThatWhenKeyDeserializerIsNotSetStringIsUsed() {
        MapBasedConfig config = commonConsumerConfiguration();
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage.produceStrings(1, null, () -> new ProducerRecord<>(topic, "my-key", "hello"));

        await().until(() -> list.size() == 1);
        assertThat(list).hasSize(1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<String, String> record = (KafkaRecord<String, String>) m;
            assertThat(record.getKey()).isEqualTo("my-key");
            assertThat(record.getPayload()).isEqualTo("hello");
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });
    }

    @Test
    public void testKeyDeserializationFailureWhenNoDeserializerSet() {
        MapBasedConfig config = commonConsumerConfiguration();
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage
                .produce(UUID.randomUUID().toString(), 1, new DoubleSerializer(), new StringSerializer(),
                        null, () -> new ProducerRecord<>(topic, 698745231.56, "hello"));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<String, String> record = (KafkaRecord<String, String>) m;
            // Deserialization provides something non printable
            assertThat(record.getKey()).isNotNull();
            assertThat(record.getPayload()).isEqualTo("hello");
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
    public void testKeyDeserializationFailureWithDeserializerSet() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("key.deserializer", JsonObjectDeserializer.class.getName());
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage
                .produce(UUID.randomUUID().toString(), 1, new DoubleSerializer(), new StringSerializer(),
                        null, () -> new ProducerRecord<>(topic, 698745231.56, "hello"));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<JsonObject, String> record = (KafkaRecord<JsonObject, String>) m;
            assertThat(record.getKey()).isEqualTo(null);
            assertThat(record.getPayload()).isEqualTo("hello");
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produce(UUID.randomUUID().toString(), 1, new JsonObjectSerializer(), new StringSerializer(),
                null, () -> new ProducerRecord<>(topic, new JsonObject().put("k", "my-key"), "hello-2"));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<JsonObject, String> record = (KafkaRecord<JsonObject, String>) list.get(1);
        // Deserialization failure - no handler
        assertThat(record.getKey()).isEqualTo(new JsonObject().put("k", "my-key"));
        assertThat(record.getPayload()).isEqualTo("hello-2");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testThatUnderlyingDeserializerReceiveTheConfiguration() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("key.deserializer", ConstantDeserializer.class.getName())
                .with("deserializer.value", "constant");
        source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> list = new ArrayList<>();
        source.getStream()
                .subscribe().with(list::add);

        usage
                .produce(UUID.randomUUID().toString(), 1, new DoubleSerializer(), new StringSerializer(),
                        null, () -> new ProducerRecord<>(topic, 698745231.56, "hello"));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<String, String> record = (KafkaRecord<String, String>) m;
            assertThat(record.getKey()).isEqualTo("constant");
            assertThat(record.getPayload()).isEqualTo("hello");
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produce(UUID.randomUUID().toString(), 1, new JsonObjectSerializer(), new StringSerializer(),
                null, () -> new ProducerRecord<>(topic, new JsonObject().put("k", "my-key"), "hello-2"));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<String, String> record = (KafkaRecord<String, String>) list.get(1);
        assertThat(record.getKey()).isEqualTo("constant");
        assertThat(record.getPayload()).isEqualTo("hello-2");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testKeyDeserializationFailureWithMatchingHandler() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("key.deserializer", JsonObjectDeserializer.class.getName())
                .with("key-deserialization-failure-handler", "my-deserialization-handler");

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
                                assertThat(KeyDeserializerConfigurationTest.this.topic).isEqualTo(topic);
                                assertThat(deserializer).isEqualTo(JsonObjectDeserializer.class.getName());
                                assertThat(data).isNotEmpty();
                                assertThat(isKey).isTrue();

                                assertThat(getHeader(headers, DeserializerWrapper.DESERIALIZATION_FAILURE_IS_KEY))
                                        .isEqualTo("true");
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
                .produce(UUID.randomUUID().toString(), 1, new DoubleSerializer(), new StringSerializer(),
                        null, () -> new ProducerRecord<>(topic, 698745231.56, "hello"));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<JsonObject, String> record = (KafkaRecord<JsonObject, String>) m;
            assertThat(record.getKey()).isEqualTo(fallback);
            assertThat(record.getPayload()).isEqualTo("hello");
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produce(UUID.randomUUID().toString(), 1, new JsonObjectSerializer(), new StringSerializer(),
                null, () -> new ProducerRecord<>(topic, new JsonObject().put("k", "my-key"), "hello-2"));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<JsonObject, String> record = (KafkaRecord<JsonObject, String>) list.get(1);
        // Deserialization failure - no handler
        assertThat(record.getKey()).isEqualTo(new JsonObject().put("k", "my-key"));
        assertThat(record.getPayload()).isEqualTo("hello-2");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testKeyDeserializationFailureWithMatchingHandlerReturningNull() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("key.deserializer", JsonObjectDeserializer.class.getName())
                .with("key-deserialization-failure-handler", "my-deserialization-handler");

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
                .produce(UUID.randomUUID().toString(), 1, new DoubleSerializer(), new StringSerializer(),
                        null, () -> new ProducerRecord<>(topic, 698745231.56, "hello"));

        await().until(() -> list.size() == 1);

        assertThat(list).allSatisfy(m -> {
            assertThat(m).isInstanceOf(KafkaRecord.class);
            KafkaRecord<JsonObject, String> record = (KafkaRecord<JsonObject, String>) m;
            assertThat(record.getKey()).isNull();
            assertThat(record.getPayload()).isEqualTo("hello");
            assertThat(record.getPartition()).isEqualTo(0);
            m.ack().toCompletableFuture().join();
        });

        usage.produce(UUID.randomUUID().toString(), 1, new JsonObjectSerializer(), new StringSerializer(),
                null, () -> new ProducerRecord<>(topic, new JsonObject().put("k", "my-key"), "hello-2"));

        await().until(() -> list.size() == 2);
        assertThat(list.get(1)).isInstanceOf(KafkaRecord.class);
        KafkaRecord<JsonObject, String> record = (KafkaRecord<JsonObject, String>) list.get(1);
        // Deserialization failure - no handler
        assertThat(record.getKey()).isEqualTo(new JsonObject().put("k", "my-key"));
        assertThat(record.getPayload()).isEqualTo("hello-2");
        assertThat(record.getPartition()).isEqualTo(0);
        list.get(1).ack().toCompletableFuture().join();
    }

    @Test
    public void testKeyDeserializationFailureWithNoMatchingHandler() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("key.deserializer", JsonObjectDeserializer.class.getName())
                .with("key-deserialization-failure-handler", "my-deserialization-handler");

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
                .with("key.deserializer", JsonObjectDeserializer.class.getName())
                .with("key-deserialization-failure-handler", "my-deserialization-handler");

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
    public void testKeyDeserializerFailsDuringConfig() {
        MapBasedConfig config = commonConsumerConfiguration()
                .with("key.deserializer", BrokenDeserializerFailingDuringConfig.class.getName());

        assertThatThrownBy(() -> source = new KafkaSource<>(vertx, "my-group",
                new KafkaConnectorIncomingConfiguration(config), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1)).isInstanceOf(KafkaException.class)
                        .hasStackTraceContaining("boom");

    }

    private MapBasedConfig commonConsumerConfiguration() {
        return new MapBasedConfig()
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
