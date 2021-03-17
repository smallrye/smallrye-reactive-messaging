package io.smallrye.reactive.messaging.kafka.fault;

import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.strimzi.StrimziKafkaContainer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

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

        MyEmitter application = runApplication(KafkaMapBasedConfig.builder()
                .put("mp.messaging.outgoing.out.connector", "smallrye-kafka")
                .put("mp.messaging.outgoing.out.bootstrap.servers", servers)
                .put("mp.messaging.outgoing.out.topic", "wrong-topic")
                .put("mp.messaging.outgoing.out.value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .put("mp.messaging.outgoing.out.delivery.timeout.ms", 1)
                .put("mp.messaging.outgoing.out.request.timeout.ms", 1)
                .put("mp.messaging.outgoing.out.acks", "all")
                .build(), MyEmitter.class);

        CompletionStage<Void> stage = application.emit("hello");

        assertThatThrownBy(() -> stage.toCompletableFuture().join()).hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Retries exhausted").hasMessageContaining("expiration");
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
