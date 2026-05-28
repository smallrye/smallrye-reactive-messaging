package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.ProcessingOrder;
import io.smallrye.reactive.messaging.kafka.TopicPartitionKey;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

class OrderedStreamHandlerTest {

    private static final String TOPIC = "ordered-race-topic";

    @Test
    void shouldCompleteWhenBufferedGroupIsRevokedBeforeItIsSubscribed() throws Exception {
        Vertx vertx = Vertx.vertx();
        OrderedStreamHandler handler = orderedStreamHandler(vertx);
        try {
            AtomicReference<MultiEmitter<? super IncomingKafkaRecord<String, String>>> emitterRef = new AtomicReference<>();
            Multi<IncomingKafkaRecord<String, String>> source = Multi.createFrom()
                    .<IncomingKafkaRecord<String, String>> emitter(emitterRef::set);

            AssertSubscriber<IncomingKafkaRecord<String, String>> subscriber = handler.decorateStream(source)
                    .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

            await().untilAsserted(() -> assertThat(emitterRef).hasValueSatisfying(emitter -> assertThat(emitter).isNotNull()));

            emitterRef.get().emit(record("key-0", 0));
            subscriber.awaitItems(1, Duration.ofSeconds(1));
            assertThat(subscriber.getItems())
                    .extracting(IncomingKafkaRecord::getKey)
                    .containsExactly("key-0");

            emitterRef.get().emit(record("key-1", 1));
            await().untilAsserted(() -> assertThat(handler.orderedByGroups)
                    .containsKey(TopicPartitionKey.ofKey(TOPIC, 0, "key-1")));

            handler.partitionsRevoked(List.of(new TopicPartition(TOPIC, 0)));
            emitterRef.get().complete();

            subscriber.awaitCompletion(Duration.ofSeconds(1));
            assertThat(subscriber.getItems())
                    .extracting(IncomingKafkaRecord::getKey)
                    .containsExactly("key-0");
        } finally {
            handler.terminate(false);
            vertx.closeAndAwait();
        }
    }

    private OrderedStreamHandler orderedStreamHandler(Vertx vertx) {
        MapBasedConfig config = new MapBasedConfig()
                .with("channel-name", "channel")
                .with("graceful-shutdown", false)
                .with("topic", TOPIC)
                .with("health-enabled", false)
                .with("tracing-enabled", false)
                .with("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .with("max-queue-size-factor", 1)
                .with("ordered.max-concurrency", 1);

        @SuppressWarnings({ "rawtypes", "unchecked" })
        KafkaConsumer<?, ?> consumer = mock(KafkaConsumer.class);
        doReturn(Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2")).when(consumer).configuration();

        return new OrderedStreamHandler(
                new KafkaConnectorIncomingConfiguration(config),
                vertx,
                consumer,
                new PassThroughCommitHandler(),
                ProcessingOrder.KEY);
    }

    private IncomingKafkaRecord<String, String> record(String key, long offset) {
        return new IncomingKafkaRecord<>(
                new ConsumerRecord<>(TOPIC, 0, offset, key, "value-" + offset),
                "channel",
                0,
                new PassThroughCommitHandler(),
                new IgnoredFailureHandler(),
                false);
    }

    private static class PassThroughCommitHandler implements KafkaCommitHandler {
        @Override
        public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
            return Uni.createFrom().voidItem();
        }
    }

    private static class IgnoredFailureHandler implements KafkaFailureHandler {
        @Override
        public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {
            return Uni.createFrom().voidItem();
        }
    }
}
