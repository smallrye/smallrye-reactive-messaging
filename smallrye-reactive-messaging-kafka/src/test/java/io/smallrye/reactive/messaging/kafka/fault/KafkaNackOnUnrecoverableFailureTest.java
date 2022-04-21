package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;

public class KafkaNackOnUnrecoverableFailureTest extends KafkaCompanionTestBase {

    @Test
    public void testNoRetryOnUnrecoverableExceptions() {
        MyEmitter application = runApplication(kafkaConfig("mp.messaging.outgoing.out")
                .put("topic", topic)
                .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .put("max.request.size", 1) // Force the failure.
                , MyEmitter.class);

        CompletionStage<Void> stage = application.emit("hello");

        assertThatThrownBy(() -> stage.toCompletableFuture().join()).hasCauseInstanceOf(RecordTooLargeException.class)
                .hasMessageNotContaining("retries").hasMessageNotContaining("/");
    }

    @Test
    public void testNoRetryOnSerializationFailure() {
        MyEmitter application = runApplication(kafkaConfig("mp.messaging.outgoing.out")
                .put("topic", topic)
                .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"), MyEmitter.class);

        CompletionStage<Void> stage = application.emitBrokenPayload();

        assertThatThrownBy(() -> stage.toCompletableFuture().join()).hasCauseInstanceOf(SerializationException.class)
                .hasMessageNotContaining("retries").hasMessageNotContaining("/");
    }

    @ApplicationScoped
    public static class MyEmitter {

        @Inject
        @Channel("out")
        Emitter<String> emitter;

        public CompletionStage<Void> emit(String p) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            Message<String> message = Message.of(p, () -> {
                future.complete(null);
                return CompletableFuture.completedFuture(null);
            }, throwable -> {
                future.completeExceptionally(throwable);
                return CompletableFuture.completedFuture(null);
            });
            emitter.send(message);
            return future;
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public CompletionStage<Void> emitBrokenPayload() {
            CompletableFuture<Void> future = new CompletableFuture<>();
            Message message = Message.of(1234455, () -> {
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
