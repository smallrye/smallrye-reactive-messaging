package io.smallrye.reactive.messaging.amqp.reply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
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

import io.smallrye.common.annotation.Identifier;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;
import io.smallrye.reactive.messaging.amqp.RabbitMQBrokerTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonLinkOptions;

public class AmqpRabbitMQRequestReplyLinkPairingTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            AmqpConnector connector = container.getBeanManager().createInstance()
                    .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME))
                    .get();
            connector.getClients().values().forEach(h -> h.client().closeAndAwait());
            connector.terminate(null);
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

    private void startReplyServer() {
        startReplyServer("requests");
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

    @Test
    public void testReplyWithLinkPairingReactiveServer() {
        String queue = "reactive-requests-" + UUID.randomUUID();
        createQueue(queue);
        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class, ReactiveReplyMessageServer.class);
        commonConfig()
                // Client config
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue)
                .with("mp.messaging.outgoing.request-reply.container-id", "request-reply")
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
                .with("mp.messaging.outgoing.request-reply.reply.address", "reactive-replies")
                // Server incoming config
                .with("mp.messaging.incoming.server-in.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.server-in.address", "/queues/" + queue)
                // Server outgoing config
                .with("mp.messaging.outgoing.server-out.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.server-out.address", "reactive-replies")
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
                .allSatisfy(r -> assertThat(r).startsWith("reply-"))
                .containsExactlyInAnyOrder("reply-0", "reply-1", "reply-2", "reply-3", "reply-4");
    }

    @Test
    public void testReplyWithLinkPairingSimpleReactiveServer() {
        UUID id = UUID.randomUUID();
        String queue = "reactive-simple-requests" + id;
        String replyQueue = "simple-reactive-replies" + id;
        createQueue(queue);
        createQueue(replyQueue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class, ReactiveReplyServer.class);
        commonConfig()
                // Client config
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue)
                .with("mp.messaging.outgoing.request-reply.container-id", "request-reply")
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
                .with("mp.messaging.outgoing.request-reply.reply.address", "/queues/" + replyQueue)
                // Server incoming config
                .with("mp.messaging.incoming.server-in.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.server-in.address", "/queues/" + queue)
                // Server outgoing config
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
    }

    @Test
    public void testReplyWithLinkPairingPairedReactiveServer() {
        String id = UUID.randomUUID().toString();
        String queue = "paired-reactive-requests-" + id;
        createQueue(queue);
        String clientContainerId = "request-reply-" + id;
        String serverContainerId = "reply-server-" + id;
        String serverLinkName = "reply-server-link-" + id;

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class, ReactiveReplyServer.class);
        commonConfig()
                // Client config
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue)
                .with("mp.messaging.outgoing.request-reply.container-id", clientContainerId)
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
                // Server incoming config
                .with("mp.messaging.incoming.server-in.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.server-in.address", "/queues/" + queue)
                .with("mp.messaging.incoming.server-in.container-id", serverContainerId)
                .with("mp.messaging.incoming.server-in.link-name", serverLinkName)
                .with("mp.messaging.incoming.server-in.link-pairing", true)
                // Server outgoing config
                .with("mp.messaging.outgoing.server-out.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.server-out.address", "$me")
                .with("mp.messaging.outgoing.server-out.container-id", serverContainerId)
                .with("mp.messaging.outgoing.server-out.link-name", serverLinkName)
                .with("mp.messaging.outgoing.server-out.link-pairing", true)
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
    }

    @Test
    public void testReplyWithLinkPairingStandardServer() {
        String queue = "requests-" + UUID.randomUUID();
        createQueue(queue);
        startReplyServer(queue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue)
                .with("mp.messaging.outgoing.request-reply.container-id", "request-reply")
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
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

    @Test
    public void testReplyWithLinkPairing() {
        String queue = "paired-requests" + UUID.randomUUID();
        createQueue(queue);
        startPairedReplyServer(queue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue)
                .with("mp.messaging.outgoing.request-reply.container-id", "request-reply")
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
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
                .untilAsserted(() -> assertThat(replies).hasSize(5)
                        .allSatisfy(r -> assertThat(r).startsWith("reply-")));
    }

    @Test
    public void testReplyWithLinkPairingMultipleEmitters() {
        String queue1 = "requests1-" + UUID.randomUUID();
        String replyQueue1 = "replies1-" + UUID.randomUUID();
        String queue2 = "requests2-" + UUID.randomUUID();
        String replyQueue2 = "replies2-" + UUID.randomUUID();
        createQueue(queue1);
        createQueue(queue2);
        createQueue(replyQueue1);
        createQueue(replyQueue2);
        startReplyServer(queue1);
        startReplyServer(queue2);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducerSecond.class);
        commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue1)
                .with("mp.messaging.outgoing.request-reply.container-id", "request-reply")
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
                .with("mp.messaging.outgoing.request-reply.reply.address", "/queues/" + replyQueue1)
                .with("mp.messaging.outgoing.request-reply2.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply2.address", "/queues/" + queue1)
                .with("mp.messaging.outgoing.request-reply2.container-id", "request-reply2")
                .with("mp.messaging.outgoing.request-reply2.link-pairing", true)
                .with("mp.messaging.outgoing.request-reply2.reply.address", "/queues/" + replyQueue2)
                .write();
        container = weld.initialize();

        List<String> replies1 = new CopyOnWriteArrayList<>();
        List<String> replies2 = new CopyOnWriteArrayList<>();
        RequestReplyProducerSecond app = container.getBeanManager().createInstance()
                .select(RequestReplyProducerSecond.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        for (int i = 0; i < 20; i++) {
            if (i % 2 == 0) {
                app.requestReply().request(Message.of(i)).subscribe().with(m -> replies1.add(m.getPayload()));
            } else {
                app.requestReply2().request(Message.of(i)).subscribe().with(m -> replies2.add(m.getPayload()));
            }
        }

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    assertThat(replies1).hasSize(10);
                    assertThat(replies2).hasSize(10);
                });
        assertThat(replies1)
                .containsExactlyInAnyOrder("0", "2", "4", "6", "8", "10", "12", "14", "16", "18");
        assertThat(replies2)
                .containsExactlyInAnyOrder("1", "3", "5", "7", "9", "11", "13", "15", "17", "19");
        assertThat(app.requestReply().getPendingReplies()).isEmpty();
        assertThat(app.requestReply2().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMultiWithLinkPairing() {
        String queue = "queue-" + UUID.randomUUID();
        createQueue(queue);
        startMultiReplyServer(queue, 10);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue)
                .with("mp.messaging.outgoing.request-reply.container-id", "request-reply")
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
                .write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        List<String> expected = new ArrayList<>();
        int sent = 3;
        for (int i = 0; i < sent; i++) {
            producer.requestReply().requestMulti(i)
                    .subscribe()
                    .with(replies::add);
            for (int j = 0; j < 10; j++) {
                expected.add(i + ": " + j);
            }
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(10 * sent));
        assertThat(replies).containsAll(expected);
        Map<CorrelationId, PendingReply> pendingReplies = producer.requestReply().getPendingReplies();
        for (PendingReply pending : pendingReplies.values()) {
            pending.complete();
        }
        await().untilAsserted(() -> assertThat(producer.requestReply().getPendingReplies()).isEmpty());
    }

    @Test
    public void testReplyWithLinkPairingSharesConnection() {
        String queue = "requests-" + UUID.randomUUID();
        createQueue(queue);
        startReplyServer("/queues/" + queue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue)
                .with("mp.messaging.outgoing.request-reply.container-id", "request-reply")
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
                .write();
        container = weld.initialize();

        AmqpConnector connector = container.getBeanManager().createInstance()
                .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> isAmqpConnectorReady(container));

        assertThat(connector.getClients()).hasSize(1);

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        for (int i = 0; i < 5; i++) {
            producer.requestReply().request(i).subscribe().with(replies::add);
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(5));
    }

    @Test
    public void testReplyWithLinkPairingWithoutContainerIdSeparateConnections() {
        String queue = "requests-" + UUID.randomUUID();
        startReplyServer(queue);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "/queues/" + queue)
                .with("mp.messaging.outgoing.request-reply.link-pairing", true)
                .write();
        container = weld.initialize();

        AmqpConnector connector = container.getBeanManager().createInstance()
                .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> isAmqpConnectorReady(container));

        assertThat(connector.getClients()).hasSize(2);

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        for (int i = 0; i < 5; i++) {
            producer.requestReply().request(i).subscribe().with(replies::add);
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(5));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4");
    }

    private void startPairedReplyServer(String address) {
        CountDownLatch ready = new CountDownLatch(1);
        Map<Symbol, Object> props = Map.of(AmqpConnector.PAIRED_KEY, true);

        ProtonClient.create(executionHolder.vertx().getDelegate())
                .connect(host, port, username, password, connectionResult -> {
                    if (connectionResult.succeeded()) {
                        var conn = connectionResult.result();
                        conn.setOfferedCapabilities(new Symbol[] { Symbol.getSymbol("LINK_PAIR_V1_0") });
                        conn.setContainer("paired-reply-server");
                        conn.openHandler(openResult -> {
                            var sender = conn.createSender("$me",
                                    new ProtonLinkOptions().setLinkName("request-reply"));
                            sender.setProperties(props);
                            sender.setAutoDrained(true);
                            sender.open();

                            var receiver = conn.createReceiver(address,
                                    new ProtonLinkOptions().setLinkName("request-reply"));
                            receiver.setProperties(props);
                            receiver.handler((delivery, msg) -> {
                                Object correlationId = msg.getMessageId();
                                if (correlationId != null) {
                                    Object body = ((AmqpValue) msg.getBody()).getValue();
                                    org.apache.qpid.proton.message.Message reply = org.apache.qpid.proton.message.Message.Factory
                                            .create();
                                    reply.setCorrelationId(correlationId);
                                    reply.setBody(new AmqpValue("reply-" + body));
                                    sender.send(reply);
                                }
                            });
                            receiver.open();
                            ready.countDown();
                        });
                        conn.open();
                    } else {
                        connectionResult.cause().printStackTrace();
                    }
                });

        try {
            ready.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startMultiReplyServer(String address, int replyCount) {
        usage.client.createReceiver(address)
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .subscribe().with(request -> {
                    String replyTo = request.replyTo();
                    String correlationId = request.id();
                    if (replyTo != null && correlationId != null) {
                        usage.client.createSender(replyTo)
                                .subscribe().with(sender -> {
                                    String payload = String.valueOf(request.bodyAsInteger());
                                    for (int i = 0; i < replyCount; i++) {
                                        sender.sendWithAck(AmqpMessage.create()
                                                .correlationId(correlationId)
                                                .withBody(payload + ": " + i)
                                                .build()).subscribe().with(x -> {
                                                });
                                    }
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

    @ApplicationScoped
    public static class RequestReplyProducerSecond {

        @Inject
        @Channel("request-reply")
        AmqpRequestReply<Integer, String> requestReply;

        @Inject
        @Channel("request-reply2")
        AmqpRequestReply<Integer, String> requestReply2;

        public AmqpRequestReply<Integer, String> requestReply() {
            return requestReply;
        }

        public AmqpRequestReply<Integer, String> requestReply2() {
            return requestReply2;
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

    @ApplicationScoped
    public static class ReactiveReplyServer {

        @Incoming("server-in")
        @Outgoing("server-out")
        public String process(int request) {
            return "reply-" + request;
        }
    }

    @ApplicationScoped
    @Identifier("my-reply-error")
    public static class MyReplyFailureHandler implements ReplyFailureHandler {

        @Override
        public Throwable handleReply(io.smallrye.reactive.messaging.amqp.AmqpMessage<?> replyMessage) {
            io.vertx.core.json.JsonObject properties = replyMessage.getApplicationProperties();
            if (properties != null) {
                String error = properties.getString("REPLY_ERROR");
                if (error != null) {
                    return new IllegalArgumentException(error);
                }
            }
            return null;
        }
    }

}
