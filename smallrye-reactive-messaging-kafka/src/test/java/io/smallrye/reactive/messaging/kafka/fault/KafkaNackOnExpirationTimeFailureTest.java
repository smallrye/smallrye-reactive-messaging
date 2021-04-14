package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.strimzi.StrimziKafkaContainer;

public class KafkaNackOnExpirationTimeFailureTest extends KafkaTestBase {

    private static int port;
    private static String servers;

    private GenericContainer<?> kafka;

    @BeforeAll
    public static void getFreePort() {
        StrimziKafkaContainer kafka = new StrimziKafkaContainer();
        kafka.start();
        await().until(kafka::isRunning);
        servers = kafka.getBootstrapServers();
        port = kafka.getMappedPort(KAFKA_PORT);
        kafka.close();
        await().until(() -> !kafka.isRunning());

    }

    @Test
    public void testExpiresAfterDeliveryTimeout() {
        usage.setBootstrapServers(servers);
        // TODO TOO LONG!
        MyEmitter application = runApplication(KafkaMapBasedConfig.builder()
                .put("mp.messaging.outgoing.out.connector", "smallrye-kafka")
                .put("mp.messaging.outgoing.out.bootstrap.servers", servers)
                .put("mp.messaging.outgoing.out.topic", "wrong-topic")
                .put("mp.messaging.outgoing.out.value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .put("mp.messaging.outgoing.out.delivery.timeout.ms", 1)
                .put("mp.messaging.outgoing.out.request.timeout.ms", 1)
                .put("mp.messaging.outgoing.out.socket.connection.setup.timeout.ms", 100)
                .put("mp.messaging.outgoing.out.reconnect.backoff.max.ms", 10000)
                .put("mp.messaging.outgoing.out.reconnect.backoff.ms", 5000)
                .put("mp.messaging.outgoing.out.transaction.timeout.ms", 1000)
                .put("mp.messaging.outgoing.out.max.block.ms", 1000)
                .put("mp.messaging.outgoing.out.retry.backoff.ms", 10)
                .put("mp.messaging.outgoing.out.acks", "all")
                .put("mp.messaging.outgoing.out.retries", 1)
                .build(), MyEmitter.class);

        CompletionStage<Void> stage = application.emit("hello");

        assertThatThrownBy(() -> stage.toCompletableFuture().join()).hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Retries exhausted");
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
    }
}
