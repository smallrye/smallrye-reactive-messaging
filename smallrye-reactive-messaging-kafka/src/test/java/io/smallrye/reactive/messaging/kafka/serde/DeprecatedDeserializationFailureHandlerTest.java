package io.smallrye.reactive.messaging.kafka.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.converters.RecordConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.JsonObjectDeserializer;

// this entire file should be removed when support for the `@Named` annotation is removed

public class DeprecatedDeserializationFailureHandlerTest extends KafkaCompanionTestBase {

    static JsonObject fallbackForValue = new JsonObject().put("fallback", "fallback");
    static JsonObject fallbackForKey = new JsonObject().put("fallback", "key");

    @Test
    public void testWhenBothValueAndKeyFailureHandlerAreSetToTheSameHandler() {
        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("topic", topic)
                .with("auto.offset.reset", "earliest")
                .with("health-enabled", false)
                .with("value.deserializer", JsonObjectDeserializer.class.getName())
                .with("key.deserializer", JsonObjectDeserializer.class.getName())
                .with("value-deserialization-failure-handler", "value-fallback")
                .with("key-deserialization-failure-handler", "key-fallback");

        addBeans(MyKeyDeserializationFailureHandler.class, MyValueDeserializationFailureHandler.class, RecordConverter.class);
        MySink sink = runApplication(config, MySink.class);

        // Fail for value
        JsonObject key = new JsonObject().put("key", "key");
        companion.produce(JsonObject.class, Double.class).fromRecords(new ProducerRecord<>(topic, key, 698745231.56));
        await().until(() -> sink.list().size() == 1);

        assertThat(sink.list().get(0)).isInstanceOf(Record.class)
                .satisfies(rec -> {
                    assertThat(rec.value()).isEqualTo(fallbackForValue);
                    assertThat(rec.key()).isEqualTo(key);
                });

        // Fail for key
        JsonObject value = new JsonObject().put("value", "value");
        companion.produce(Double.class, JsonObject.class).fromRecords(new ProducerRecord<>(topic, 698745231.56, value));
        await().until(() -> sink.list().size() == 2);

        assertThat(sink.list().get(1)).isInstanceOf(Record.class)
                .satisfies(rec -> {
                    assertThat(rec.value()).isEqualTo(value);
                    assertThat(rec.key()).isEqualTo(fallbackForKey);
                });

        // Everything ok
        companion.produce(JsonObject.class, JsonObject.class).fromRecords(new ProducerRecord<>(topic, key, value));

        await().until(() -> sink.list().size() == 3);
        assertThat(sink.list().get(2)).isInstanceOf(Record.class)
                .satisfies(rec -> {
                    assertThat(rec.value()).isEqualTo(value);
                    assertThat(rec.key()).isEqualTo(key);
                });

        // Fail both
        companion.produce(Double.class, Double.class).fromRecords(new ProducerRecord<>(topic, 23.54, 145.56));

        await().until(() -> sink.list().size() == 4);
        assertThat(sink.list().get(3)).isInstanceOf(Record.class)
                .satisfies(rec -> {
                    assertThat(rec.value()).isEqualTo(fallbackForValue);
                    assertThat(rec.key()).isEqualTo(fallbackForKey);
                });
    }

    @ApplicationScoped
    public static class MySink {
        List<Record<JsonObject, JsonObject>> list = new ArrayList<>();

        @Incoming("kafka")
        public void consume(Record<JsonObject, JsonObject> record) {
            list.add(record);
        }

        public List<Record<JsonObject, JsonObject>> list() {
            return list;
        }
    }

    @ApplicationScoped
    @Named("key-fallback")
    public static class MyKeyDeserializationFailureHandler implements DeserializationFailureHandler<JsonObject> {

        @Override
        public JsonObject handleDeserializationFailure(String topic, boolean isKey, String deserializer, byte[] data,
                Exception exception, Headers headers) {
            return fallbackForKey;
        }
    }

    @ApplicationScoped
    @Named("value-fallback")
    public static class MyValueDeserializationFailureHandler implements DeserializationFailureHandler<JsonObject> {
        @Override
        public JsonObject handleDeserializationFailure(String topic, boolean isKey, String deserializer, byte[] data,
                Exception exception, Headers headers) {
            return fallbackForValue;
        }
    }
}
