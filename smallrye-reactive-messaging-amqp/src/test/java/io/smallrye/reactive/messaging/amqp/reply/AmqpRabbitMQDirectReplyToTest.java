package io.smallrye.reactive.messaging.amqp.reply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.RabbitMQBrokerTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpReceiver;

public class AmqpRabbitMQDirectReplyToTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.getBeanManager().createInstance()
                    .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME))
                    .get().terminate(null);
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    private MapBasedConfig commonConfig() {
        return new MapBasedConfig()
                .with("mp.messaging.connector.smallrye-amqp.host", host)
                .with("mp.messaging.connector.smallrye-amqp.port", port)
                .with("mp.messaging.connector.smallrye-amqp.username", username)
                .with("mp.messaging.connector.smallrye-amqp.password", password)
                .with("mp.messaging.connector.smallrye-amqp.tracing-enabled", false);
    }

    @Test
    public void testReplyWithDirectReplyTo() {
        startReplyServer("direct-reply-requests");

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "direct-reply-requests")
                .with("mp.messaging.outgoing.request-reply.container-id", "request-reply")
                .with("mp.messaging.outgoing.request-reply.use-anonymous-sender", false)
                .with("mp.messaging.outgoing.request-reply.reply.address", "amq.rabbitmq.reply-to")
                .write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));
        for (int i = 0; i < 5; i++) {
            producer.requestReply().request(i).subscribe().with(replies::add);
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(5));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4");
    }

    private void startReplyServer(String address) {
        usage.client.createReceiver(address)
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .subscribe().with(request -> {
                    String replyTo = request.replyTo();
                    String correlationId = request.id();
                    if (replyTo != null && correlationId != null) {
                        usage.client.createSender(replyTo)
                                .onItem().transformToUni(sender -> sender.sendWithAck(AmqpMessage.create()
                                        .correlationId(correlationId)
                                        .withBody(String.valueOf(request.bodyAsInteger()))
                                        .build()))
                                .subscribe().with(x -> {
                                });
                    }
                });
    }

    @ApplicationScoped
    public static class RequestReplyProducer {

        @Inject
        @Channel("request-reply")
        AmqpRequestReply<Integer, String> requestReply;

        public AmqpRequestReply<Integer, String> requestReply() {
            return requestReply;
        }
    }

}
