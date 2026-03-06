package io.smallrye.reactive.messaging.rabbitmq.internals;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class RabbitMQMessageSenderTest extends WeldTestBase {

    private MapBasedConfig outgoingConfig() {
        return commonConfig()
                .with("mp.messaging.outgoing.sink.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.exchange.name", exchangeName)
                .with("mp.messaging.outgoing.sink.exchange.declare", false)
                .with("mp.messaging.outgoing.sink.default-routing-key", routingKeys)
                .with("mp.messaging.outgoing.sink.tracing.enabled", false);
    }

    @Test
    void sendingMessages() {
        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKeys, received::add);

        SenderBean bean = runApplication(outgoingConfig(), SenderBean.class);
        for (int i = 1; i <= 5; i++) {
            bean.send(i);
        }

        await().until(() -> received.size() >= 5);
        assertThat(received).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    }

    @Test
    void sendingWithDefaultTtl() {
        List<String> expirations = new CopyOnWriteArrayList<>();
        usage.consume(exchangeName, routingKeys, msg -> {
            expirations.add(msg.properties().getExpiration());
        });

        SenderBean bean = runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.sink.default-ttl", 5000L),
                SenderBean.class);
        for (int i = 1; i <= 3; i++) {
            bean.send(i);
        }

        await().until(() -> expirations.size() >= 3);
        assertThat(expirations).allMatch("5000"::equals);
    }

    @Test
    void sendingWithPublishConfirms() {
        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKeys, received::add);

        SenderBean bean = runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.sink.publish-confirms", true),
                SenderBean.class);
        for (int i = 1; i <= 5; i++) {
            bean.send(i);
        }

        await().until(() -> received.size() >= 5);
        assertThat(received).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    }

    @Test
    void sendingWithMaxInflightMessages() {
        List<Integer> received = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(exchangeName, routingKeys, received::add);

        SenderBean bean = runApplication(outgoingConfig()
                .with("mp.messaging.outgoing.sink.max-inflight-messages", 2),
                SenderBean.class);
        for (int i = 1; i <= 10; i++) {
            bean.send(i);
        }

        await().until(() -> received.size() >= 10);
        assertThat(received).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    // --- Inner beans ---

    @ApplicationScoped
    public static class SenderBean {
        @Inject
        @Channel("sink")
        Emitter<Integer> emitter;

        public void send(int value) {
            emitter.send(value);
        }
    }

}
