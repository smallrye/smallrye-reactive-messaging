package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionProxyTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class ProducerRetryTest extends KafkaCompanionProxyTestBase {

    private KafkaMapBasedConfig getBaseConfig() {
        return kafkaConfig("mp.messaging.outgoing.kafka")
                .put("topic", topic)
                .put("value.serializer", IntegerSerializer.class.getName());
    }

    @Test
    public void testSendRetriesOnTransientFailure() throws Exception {
        EmitterWithAckNack application = runApplication(getBaseConfig()
                .put("send.retries", 5)
                .put("delivery.timeout.ms", 5000)
                .put("request.timeout.ms", 1000), EmitterWithAckNack.class);

        disableProxy();

        CompletionStage<Void> stage = application.emit(1);

        Thread.sleep(500);

        enableProxy();

        stage.toCompletableFuture().join();

        assertThat(companion.consumeIntegers().fromTopics(topic, 1, Duration.ofMinutes(1))
                .awaitCompletion(Duration.ofMinutes(1)).count()).isEqualTo(1);
    }

    @Test
    public void testNoSendRetriesByDefault() {
        EmitterWithAckNack application = runApplication(getBaseConfig()
                .put("max.block.ms", 1000)
                .put("delivery.timeout.ms", 1000)
                .put("request.timeout.ms", 500), EmitterWithAckNack.class);

        disableProxy();

        CompletionStage<Void> stage = application.emit(1);

        await().atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> assertThat(stage.toCompletableFuture()).isCompletedExceptionally());
    }

    @ApplicationScoped
    public static class EmitterWithAckNack {

        @Inject
        @Channel("kafka")
        Emitter<Integer> emitter;

        public CompletionStage<Void> emit(int value) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            Message<Integer> message = Message.of(value, () -> {
                future.complete(null);
                return CompletableFuture.completedFuture(null);
            }, throwable -> {
                future.completeExceptionally(throwable);
                return CompletableFuture.completedFuture(null);
            });
            emitter.send(message);
            return future;
        }
    }
}
