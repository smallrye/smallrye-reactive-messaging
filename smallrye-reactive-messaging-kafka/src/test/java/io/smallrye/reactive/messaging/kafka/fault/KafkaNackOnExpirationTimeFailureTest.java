package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class KafkaNackOnExpirationTimeFailureTest extends WeldTestBase {

    private static String servers;

    @BeforeAll
    public static void setRandomBootstrapServers() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            servers = String.format("PLAINTEXT://%s:%s", s.getInetAddress().getHostAddress(), s.getLocalPort());
        }
    }

    @Test
    public void testExpiresAfterDeliveryTimeout() throws IOException {
        // TODO TOO LONG!
        MyEmitter application = runApplication(new MapBasedConfig()
                .with("bootstrap.servers", servers)
                .with("mp.messaging.outgoing.out.connector", "smallrye-kafka")
                .with("mp.messaging.outgoing.out.bootstrap.servers", servers)
                .with("mp.messaging.outgoing.out.topic", "wrong-topic")
                .with("mp.messaging.outgoing.out.value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .with("mp.messaging.outgoing.out.delivery.timeout.ms", 1)
                .with("mp.messaging.outgoing.out.request.timeout.ms", 1)
                .with("mp.messaging.outgoing.out.socket.connection.setup.timeout.ms", 100)
                .with("mp.messaging.outgoing.out.reconnect.backoff.max.ms", 10000)
                .with("mp.messaging.outgoing.out.reconnect.backoff.ms", 5000)
                .with("mp.messaging.outgoing.out.transaction.timeout.ms", 1000)
                .with("mp.messaging.outgoing.out.max.block.ms", 1000)
                .with("mp.messaging.outgoing.out.retry.backoff.ms", 10)
                .with("mp.messaging.outgoing.out.acks", "all")
                .with("mp.messaging.outgoing.out.retries", 1), MyEmitter.class);

        CompletionStage<Void> stage = application.emit("hello");

        assertThatThrownBy(() -> stage.toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
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
