package io.smallrye.reactive.messaging.rabbitmq.og;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Tests acknowledgement chaining from incoming to outgoing channels.
 * Verifies that ack/nack propagates correctly through the message pipeline.
 */
public class AckChainTest extends WeldTestBase {

    @Test
    void testAckPropagation() {
        addBeans(MyApp.class);
        runApplication(new MapBasedConfig()
                .with("mp.messaging.outgoing.outgoing-no-ack.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.outgoing-no-ack.exchange.name", "DemoNoAck")
                .with("mp.messaging.outgoing.outgoing-no-ack.exchange.type", "topic")
                .with("mp.messaging.outgoing.outgoing-no-ack.exchange.declare", "true")
                .with("mp.messaging.outgoing.outgoing-no-ack.host", host)
                .with("mp.messaging.outgoing.outgoing-no-ack.port", port)
                .with("mp.messaging.outgoing.outgoing-no-ack.tracing.enabled", false)
                .with("mp.messaging.incoming.incoming-no-ack.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.incoming-no-ack.exchange.name", "DemoNoAck")
                .with("mp.messaging.incoming.incoming-no-ack.queue.name", "queue.no.ack")
                .with("mp.messaging.incoming.incoming-no-ack.queue.declare", "true")
                .with("mp.messaging.incoming.incoming-no-ack.routing-keys", "no.ack")
                .with("mp.messaging.incoming.incoming-no-ack.host", host)
                .with("mp.messaging.incoming.incoming-no-ack.port", port)
                .with("mp.messaging.incoming.incoming-no-ack.tracing.enabled", false)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-reconnect-attempts", 0));

        usage.produce("DemoNoAck", "queue.no.ack", "no.ack", 1, () -> "payload");
        MyApp app = container.select(MyApp.class).get();

        Awaitility.await().until(() -> app.acked());
    }

    @ApplicationScoped
    public static class MyApp {
        @Inject
        @Channel("outgoing-no-ack")
        Emitter<String> emitter;

        AtomicBoolean acked = new AtomicBoolean(false);

        @Incoming("incoming-no-ack")
        CompletableFuture<Void> consume(Message<byte[]> msg) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            Metadata metadata = Metadata.of(OutgoingRabbitMQMetadata.builder()
                    .withRoutingKey("other.queue")
                    .withContentType("text/plain")
                    .build());
            Message<String> output = Message.of(new String(msg.getPayload())).withMetadata(metadata)
                    .withAck(() -> {
                        future.complete(null);
                        acked.set(true);
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(t -> {
                        future.completeExceptionally(t);
                        return CompletableFuture.completedFuture(null);
                    });
            emitter.send(output);
            return future;
        }

        public boolean acked() {
            return acked.get();
        }
    }
}
