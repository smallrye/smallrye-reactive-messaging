package io.smallrye.reactive.messaging.amqp.reply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;
import io.smallrye.reactive.messaging.amqp.RabbitMQBrokerTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpReceiver;

public class AmqpRabbitMQRequestReplyTest extends RabbitMQBrokerTestBase {

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

    private MapBasedConfig requestReplyConfig(String requestQueue, String replyQueue) {
        return commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + requestQueue)
                .with("mp.messaging.outgoing.request-reply.use-anonymous-sender", false)
                .with("mp.messaging.outgoing.request-reply.reply.address", "/queues/" + replyQueue);
    }

    private void startReplyServer(String requestQueue, String replyQueue) {
        usage.client.createReceiver("/queues/" + requestQueue)
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

    @Test
    public void testReply() {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String requestQueue = "requests-" + id;
        String replyQueue = "replies-" + id;
        createQueue(requestQueue);
        createQueue(replyQueue);
        startReplyServer(requestQueue, replyQueue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        requestReplyConfig(requestQueue, replyQueue).write();
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
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMessage() {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String requestQueue = "requests-msg-" + id;
        String replyQueue = "replies-msg-" + id;
        createQueue(requestQueue);
        createQueue(replyQueue);
        startReplyServer(requestQueue, replyQueue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        requestReplyConfig(requestQueue, replyQueue).write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        for (int i = 0; i < 5; i++) {
            producer.requestReply().request(Message.of(i)).subscribe().with(m -> replies.add(m.getPayload()));
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(5));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyTimeout() {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String requestQueue = "requests-timeout-" + id;
        String replyQueue = "replies-timeout-" + id;
        createQueue(requestQueue);
        createQueue(replyQueue);
        // No reply server — requests will time out

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        requestReplyConfig(requestQueue, replyQueue)
                .with("mp.messaging.outgoing.request-reply." + AmqpRequestReply.REPLY_TIMEOUT_KEY, "1000")
                .write();
        container = weld.initialize();

        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        producer.requestReply().request(1)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure(Duration.ofSeconds(5)).assertFailedWith(AmqpRequestReplyTimeoutException.class);
    }

    @Test
    public void testReplyWithReactiveServer() {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String requestQueue = "requests-reactive-" + id;
        String replyQueue = "replies-reactive-" + id;
        createQueue(requestQueue);
        createQueue(replyQueue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class, ReactiveReplyServer.class);
        requestReplyConfig(requestQueue, replyQueue)
                .with("mp.messaging.incoming.server-in.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.server-in.address", "/queues/" + requestQueue)
                .with("mp.messaging.outgoing.server-out.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.server-out.address", "/queues/" + replyQueue)
                .with("mp.messaging.outgoing.server-out.use-anonymous-sender", false)
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
        assertThat(replies)
                .containsExactlyInAnyOrder("reply-0", "reply-1", "reply-2", "reply-3", "reply-4");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyWithReactiveMessageServer() {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String requestQueue = "requests-reactive-msg-" + id;
        String replyQueue = "replies-reactive-msg-" + id;
        createQueue(requestQueue);
        createQueue(replyQueue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class, ReactiveReplyMessageServer.class);
        requestReplyConfig(requestQueue, replyQueue)
                .with("mp.messaging.incoming.server-in.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.server-in.address", "/queues/" + requestQueue)
                .with("mp.messaging.outgoing.server-out.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.server-out.address", "/queues/" + replyQueue)
                .with("mp.messaging.outgoing.server-out.use-anonymous-sender", false)
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
        assertThat(replies)
                .containsExactlyInAnyOrder("reply-0", "reply-1", "reply-2", "reply-3", "reply-4");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
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

    @ApplicationScoped
    public static class ReactiveReplyServer {

        @Incoming("server-in")
        @Outgoing("server-out")
        public String process(int request) {
            return "reply-" + request;
        }
    }

    @ApplicationScoped
    public static class ReactiveReplyMessageServer {

        @Incoming("server-in")
        @Outgoing("server-out")
        public Message<String> process(Message<Integer> request) {
            IncomingAmqpMetadata incoming = request.getMetadata(IncomingAmqpMetadata.class).orElse(null);
            if (incoming != null) {
                OutgoingAmqpMetadata outMeta = OutgoingAmqpMetadata.builder()
                        .withAddress(incoming.getReplyTo())
                        .withCorrelationId(incoming.getId())
                        .build();
                return request.withPayload("reply-" + request.getPayload()).addMetadata(outMeta);
            }
            return request.withPayload(String.valueOf(request.getPayload()));
        }
    }
}
