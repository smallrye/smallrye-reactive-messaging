package io.smallrye.reactive.messaging.kafka.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.SerializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.converters.RecordConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;

class SerializationFailureHandlerTest extends KafkaCompanionTestBase {

    @Test
    void testWhenNoFailureHandlerIsSet() {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .with("topic", topic)
                .with("health-enabled", false)
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", DoubleSerializer.class.getName());

        addBeans(RecordConverter.class);

        runApplication(config, MySource.class);

        ConsumerTask<String, Double> consumed = companion.consumeDoubles().fromTopics(topic, 1);

        await().pollDelay(1, TimeUnit.SECONDS).until(() -> consumed.getRecords().size() == 0);
        assertThat(isAlive()).isTrue();
    }

    @Test
    void testWhenValueFailureHandlerIsSet() {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .with("topic", topic)
                .with("health-enabled", false)
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", DoubleSerializer.class.getName())
                .with("value-serialization-failure-handler", "recover-with-zero");

        addBeans(RecordConverter.class, RecoverSerializerFailureHandler.class);

        runApplication(config, MySource.class);

        ConsumerTask<String, Double> consumed = companion.consumeDoubles().fromTopics(topic, 1);

        await().pollDelay(1, TimeUnit.SECONDS).until(() -> consumed.getRecords().size() == 1);
        assertThat(isAlive()).isTrue();
        assertThat(consumed.getRecords().get(0)).isInstanceOf(ConsumerRecord.class)
                .satisfies(rec -> assertThat(rec.value()).isEqualTo(0.0));
    }

    @Test
    void testWhenBothValueAndKeyFailureHandlerAreSetToTheSameHandler() {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .with("topic", topic)
                .with("health-enabled", false)
                .with("key.serializer", JsonObjectSerializer.class.getName())
                .with("value.serializer", DoubleSerializer.class.getName())
                .with("value-serialization-failure-handler", "recover-with-item")
                .with("key-serialization-failure-handler", "recover-with-item");

        addBeans(RecordConverter.class, RecoverKeyValueSerializerFailureHandler.class);

        runApplication(config, MySource.class);

        ConsumerTask<String, Double> consumed = companion.consumeDoubles().fromTopics(topic, 1);

        await().pollDelay(1, TimeUnit.SECONDS).until(() -> consumed.getRecords().size() == 1);
        assertThat(isAlive()).isTrue();
        assertThat(consumed.getRecords().get(0)).isInstanceOf(ConsumerRecord.class)
                .satisfies(rec -> {
                    assertThat(rec.key()).isEqualTo("key");
                    assertThat(rec.value()).isEqualTo(0.0);
                });
    }

    @Test
    void testWhenValueFailureHandlerRetries() {
        MapBasedConfig config = kafkaConfig("mp.messaging.outgoing.kafka")
                .with("topic", topic)
                .with("health-enabled", false)
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", DoubleSerializer.class.getName())
                .with("value-serialization-failure-handler", "retry-twice-then-recover");

        addBeans(RecordConverter.class, RetrySerializerFailureHandler.class);

        runApplication(config, MySource.class);

        ConsumerTask<String, Double> consumed = companion.consumeDoubles().fromTopics(topic);

        await().pollDelay(1, TimeUnit.SECONDS).until(() -> consumed.getRecords().size() == 1);
        assertThat(isAlive()).isTrue();
        assertThat(consumed.getRecords().get(0)).isInstanceOf(ConsumerRecord.class)
                .satisfies(rec -> {
                    assertThat(rec.value()).isEqualTo(0.0);
                    assertThat(new String(rec.headers().lastHeader("retry-count").value())).isEqualTo("2");
                });
    }

    @ApplicationScoped
    public static class MySource {
        @Outgoing("kafka")
        public Multi<Record<String, JsonObject>> produce() {
            return Multi.createFrom().items(Record.of("key", new JsonObject().put("value", "value")));
        }

    }

    @Identifier("recover-with-zero")
    public static class RecoverSerializerFailureHandler implements SerializationFailureHandler<Object> {

        DoubleSerializer doubleSer = new DoubleSerializer();

        @Override
        public byte[] handleSerializationFailure(String topic, boolean isKey, String serializer, Object data,
                Exception exception, Headers headers) {
            return doubleSer.serialize(topic, 0.0);
        }
    }

    @Identifier("recover-with-item")
    public static class RecoverKeyValueSerializerFailureHandler implements SerializationFailureHandler<Object> {

        StringSerializer stringSer = new StringSerializer();
        DoubleSerializer doubleSer = new DoubleSerializer();

        @Override
        public byte[] handleSerializationFailure(String topic, boolean isKey, String serializer, Object data,
                Exception exception, Headers headers) {
            if (isKey) {
                return stringSer.serialize(topic, "key");
            } else {
                return doubleSer.serialize(topic, 0.0);
            }
        }
    }

    @Identifier("retry-twice-then-recover")
    public static class RetrySerializerFailureHandler implements SerializationFailureHandler<Object> {

        DoubleSerializer doubleSer = new DoubleSerializer();
        AtomicInteger retryCount = new AtomicInteger();

        @Override
        public byte[] decorateSerialization(Uni<byte[]> serialization, String topic, boolean isKey, String serializer,
                Object data, Headers headers) {
            return serialization
                    .onFailure().invoke(t -> retryCount.incrementAndGet())
                    .onFailure().retry().atMost(1)
                    .onFailure().recoverWithItem(() -> {
                        headers.add("retry-count", Integer.toString(retryCount.get()).getBytes());
                        return doubleSer.serialize(topic, 0.0);
                    })
                    .await().indefinitely();
        }
    }

}
