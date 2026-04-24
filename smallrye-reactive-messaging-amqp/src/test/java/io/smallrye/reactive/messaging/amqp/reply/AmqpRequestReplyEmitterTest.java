package io.smallrye.reactive.messaging.amqp.reply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

import io.smallrye.common.annotation.Identifier;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.reactive.messaging.amqp.AmqpBrokerTestBase;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpReceiver;

public class AmqpRequestReplyEmitterTest extends AmqpBrokerTestBase {

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

    private MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply.address", "requests")
                .with("mp.messaging.outgoing.request-reply.host", host)
                .with("mp.messaging.outgoing.request-reply.port", port)
                .with("mp.messaging.outgoing.request-reply.username", username)
                .with("mp.messaging.outgoing.request-reply.password", password)
                .with("mp.messaging.outgoing.request-reply.tracing-enabled", false)
                .with("mp.messaging.outgoing.request-reply.reply.address", "request-reply-reply");
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

    private void startSlowReplyServer() {
        usage.client.createReceiver("requests")
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .subscribe().with(request -> {
                    String replyTo = request.replyTo();
                    String correlationId = request.id();
                    if (replyTo != null && correlationId != null) {
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
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
        startReplyServer();

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        config().write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));
        for (int i = 0; i < 10; i++) {
            producer.requestReply().request(i).subscribe().with(replies::add);
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMessage() {
        startReplyServer();

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        config().write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        for (int i = 0; i < 10; i++) {
            producer.requestReply().request(Message.of(i)).subscribe().with(m -> replies.add(m.getPayload()));
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyTimeout() {
        startSlowReplyServer();

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        config()
                .with("mp.messaging.outgoing.request-reply." + AmqpRequestReply.REPLY_TIMEOUT_KEY, "1000")
                .write();
        container = weld.initialize();

        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        producer.requestReply().request(1)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure().assertFailedWith(AmqpRequestReplyTimeoutException.class);
    }

    @Test
    public void testReplyMessageMulti() {
        startMultiReplyServer(10);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        config().write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        List<String> expected = new ArrayList<>();
        int sent = 5;
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
    public void testReplyMessageMultiLimit() {
        startMultiReplyServer(10);

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        config().write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        producer.requestReply().requestMulti(0)
                .select().first(5)
                .subscribe()
                .with(replies::add);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(5));
        assertThat(replies)
                .containsExactlyInAnyOrder("0: 0", "0: 1", "0: 2", "0: 3", "0: 4");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMultipleEmittersSameRequestAddress() {
        startReplyServer();

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducerSecond.class);
        config()
                .with("mp.messaging.outgoing.request-reply2.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.request-reply2.address", "requests")
                .with("mp.messaging.outgoing.request-reply2.host", host)
                .with("mp.messaging.outgoing.request-reply2.port", port)
                .with("mp.messaging.outgoing.request-reply2.username", username)
                .with("mp.messaging.outgoing.request-reply2.password", password)
                .with("mp.messaging.outgoing.request-reply2.tracing-enabled", false)
                .with("mp.messaging.outgoing.request-reply2.reply.address", "request-reply2-reply")
                .write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducerSecond app = container.getBeanManager().createInstance()
                .select(RequestReplyProducerSecond.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        for (int i = 0; i < 20; i++) {
            AmqpRequestReply<Integer, String> requestReply = (i % 2 == 0) ? app.requestReply() : app.requestReply2();
            requestReply.request(Message.of(i)).subscribe().with(m -> replies.add(m.getPayload()));
        }

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(20));
        assertThat(replies)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19");
        assertThat(app.requestReply().getPendingReplies()).isEmpty();
        assertThat(app.requestReply2().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMessageBytesCorrelationId() {
        startReplyServer();

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        config()
                .with("mp.messaging.outgoing.request-reply." + AmqpRequestReply.REPLY_CORRELATION_ID_HANDLER_KEY,
                        "bytes")
                .write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));
        for (int i = 0; i < 10; i++) {
            producer.requestReply().request(Message.of(i)).subscribe().with(m -> replies.add(m.getPayload()));
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyFailureHandler() {
        startReplyServerWithFailure();

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class, MyReplyFailureHandler.class);
        config()
                .with("mp.messaging.outgoing.request-reply." + AmqpRequestReply.REPLY_FAILURE_HANDLER_KEY,
                        "my-reply-error")
                .write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        for (int i = 0; i < 10; i++) {
            producer.requestReply().request(i)
                    .subscribe().with(replies::add, errors::add);
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(6));
        assertThat(replies)
                .containsExactlyInAnyOrder("1", "2", "4", "5", "7", "8");
        await().untilAsserted(() -> assertThat(errors).hasSize(4));
        assertThat(errors)
                .extracting(Throwable::getMessage)
                .allSatisfy(message -> assertThat(message).containsAnyOf("0", "3", "6", "9")
                        .contains("Cannot reply to"));
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyGracefulShutdown() {
        startNonReplyingServer();

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyProducer.class);
        config()
                .with("mp.messaging.outgoing.request-reply." + AmqpRequestReply.REPLY_TIMEOUT_KEY, "30000")
                .write();
        container = weld.initialize();

        RequestReplyProducer producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducer.class).get();
        await().until(() -> isAmqpConnectorReady(container));

        for (int i = 0; i < 5; i++) {
            producer.requestReply().request(i)
                    .subscribe().with(r -> {
                    }, e -> {
                    });
        }
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(producer.requestReply().getPendingReplies()).hasSize(5));

        producer.requestReply().complete();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(producer.requestReply().getPendingReplies()).isEmpty());
    }

    private void startNonReplyingServer() {
        usage.client.createReceiver("requests")
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .subscribe().with(request -> {
                });
    }

    private void startMultiReplyServer(int replyCount) {
        usage.client.createReceiver("requests")
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

    private void startReplyServerWithFailure() {
        usage.client.createReceiver("requests")
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .subscribe().with(request -> {
                    String replyTo = request.replyTo();
                    String correlationId = request.id();
                    if (replyTo != null && correlationId != null) {
                        int value = request.bodyAsInteger();
                        io.vertx.mutiny.amqp.AmqpMessageBuilder builder = AmqpMessage.create()
                                .correlationId(correlationId)
                                .withBody(String.valueOf(value));
                        if (value % 3 == 0) {
                            builder.applicationProperties(
                                    io.vertx.core.json.JsonObject.of("REPLY_ERROR", "Cannot reply to " + value));
                        }
                        io.vertx.mutiny.amqp.AmqpMessage reply = builder.build();
                        usage.client.createSender(replyTo)
                                .onItem().transformToUni(sender -> sender.sendWithAck(reply))
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
