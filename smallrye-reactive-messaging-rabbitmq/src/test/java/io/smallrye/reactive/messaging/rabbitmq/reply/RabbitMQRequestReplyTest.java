package io.smallrye.reactive.messaging.rabbitmq.reply;

import static io.smallrye.reactive.messaging.rabbitmq.reply.RabbitMQRequestReply.REPLY_FAILURE_HANDLER_KEY;
import static io.smallrye.reactive.messaging.rabbitmq.reply.RabbitMQRequestReply.REPLY_TIMEOUT_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata.Builder;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQBrokerTestBase;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.converter.ByteArrayMessageConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

class RabbitMQRequestReplyTest extends RabbitMQBrokerTestBase {

    private WeldContainer container;

    Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            get(container, RabbitMQConnector.class,
                    ConnectorLiteral.of(RabbitMQConnector.CONNECTOR_NAME))
                    .terminate(null);
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    private MapBasedConfig config(String exchange, String requestAddress) {
        return commonConfig()
                .with("mp.messaging.outgoing.request-reply.connector", "smallrye-rabbitmq")
                .with("mp.messaging.outgoing.request-reply.exchange.name", exchange)
                .with("mp.messaging.outgoing.request-reply.exchange.type", "direct")
                .with("mp.messaging.outgoing.request-reply.default-routing-key", requestAddress)
                .with("mp.messaging.outgoing.request-reply.shared-connection-name", "request-reply")

                .with("mp.messaging.incoming.req.connector", "smallrye-rabbitmq")
                .with("mp.messaging.incoming.req.exchange.name", exchange)
                .with("mp.messaging.incoming.req.exchange.type", "direct")
                .with("mp.messaging.incoming.req.routing-keys", requestAddress)

                .with("mp.messaging.outgoing.rep.connector", "smallrye-rabbitmq")
                .with("mp.messaging.outgoing.rep.exchange.name", "\"\"");
    }

    @Test
    public void testReply() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducer.class, ReplyServer.class);
        config(exchange, requestAddress).write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance().select(RequestReplyProducer.class).get();
        await().until(() -> isRabbitMQConnectorAvailable(container));
        for (int i = 0; i < 10; i++) {
            producer.requestReply().request(i).subscribe().with(replies::add);
        }
        await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyWithConverter() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducerWithConverter.class, ReplyServer.class, ByteArrayMessageConverter.class);
        config(exchange, requestAddress).write();
        container = weld.initialize();

        List<byte[]> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducerWithConverter producer = container.getBeanManager().createInstance()
                .select(RequestReplyProducerWithConverter.class).get();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        for (int i = 0; i < 10; i++) {
            producer.requestReply().request(i).subscribe().with(replies::add);
        }
        await().atMost(java.time.Duration.ofSeconds(20)).untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes(), "5".getBytes(),
                "6".getBytes(), "7".getBytes(), "8".getBytes(), "9".getBytes());
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMessage() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducer.class, ReplyServer.class);
        config(exchange, requestAddress).write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer app = container.getBeanManager().createInstance().select(RequestReplyProducer.class).get();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(Message.of(i)).subscribe().with(m -> replies.add(m.getPayload()));
        }
        await().atMost(java.time.Duration.ofSeconds(20)).untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(app.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMessageMulti() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducer.class, ReplyServerMultipleReplies.class);
        config(exchange, requestAddress).write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer app = container.getBeanManager().createInstance().select(RequestReplyProducer.class).get();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        List<String> expected = new ArrayList<>();
        int sent = 5;
        for (int i = 0; i < sent; i++) {
            app.requestReply().requestMulti(i)
                    .subscribe()
                    .with(replies::add);
            for (int j = 0; j < ReplyServerMultipleReplies.REPLIES; j++) {
                expected.add(i + ": " + j);
            }
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(ReplyServerMultipleReplies.REPLIES * sent));
        assertThat(replies)
                .containsAll(expected);
        Map<CorrelationId, PendingReply> pendingReplies = app.requestReply().getPendingReplies();
        assertThat(pendingReplies).allSatisfy((k, v) -> assertThat(v.isCancelled()).isFalse());
        for (PendingReply pending : pendingReplies.values()) {
            pending.complete();
        }
        assertThat(pendingReplies).allSatisfy((k, v) -> assertThat(v.isCancelled()).isTrue());
        assertThat(app.requestReply().getPendingReplies())
                .allSatisfy((k, v) -> assertThat(v.isCancelled()).isTrue());
        await().untilAsserted(() -> assertThat(app.requestReply().getPendingReplies()).isEmpty());
    }

    @Test
    public void testReplyMessageMultiLimit() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducer.class, ReplyServerMultipleReplies.class, MyReplyFailureHandler.class);
        config(exchange, requestAddress).write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer app = container.getBeanManager().createInstance().select(RequestReplyProducer.class).get();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        app.requestReply().requestMulti(0)
                .select().first(5)
                .subscribe()
                .with(replies::add);

        await().untilAsserted(() -> assertThat(replies).hasSize(5));
        assertThat(replies)
                .containsExactlyInAnyOrder("0: 0", "0: 1", "0: 2", "0: 3", "0: 4");
        assertThat(app.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMultipleEmittersSameRequestAddress() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducerSecond.class, ReplyServer.class);
        config(exchange, requestAddress)
                .with("mp.messaging.outgoing.request-reply2.connector", "smallrye-rabbitmq")
                .with("mp.messaging.outgoing.request-reply2.exchange.name", exchange)
                .with("mp.messaging.outgoing.request-reply2.exchange.type", "direct")
                .with("mp.messaging.outgoing.request-reply2.default-routing-key", requestAddress)
                .with("mp.messaging.outgoing.request-reply2.shared-connection-name", "request-reply2")
                .write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducerSecond app = container.getBeanManager().createInstance().select(RequestReplyProducerSecond.class)
                .get();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        for (int i = 0; i < 20; i++) {
            RabbitMQRequestReply<Integer, String> requestReply = (i % 2 == 0) ? app.requestReply() : app.requestReply2();
            requestReply.request(Message.of(i)).subscribe().with(m -> {
                replies.add(m.getPayload());
            });
        }

        await().untilAsserted(() -> assertThat(replies).hasSize(20));
        assertThat(replies)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                        "16", "17", "18", "19");

        assertThat(app.requestReply().getPendingReplies()).isEmpty();
        assertThat(app.requestReply2().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyMessageBytesCorrelationId() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducer.class, ReplyServer.class);
        config(exchange, requestAddress)
                .with("mp.messaging.outgoing.request-reply.reply.correlation-id.handler", "bytes")
                .write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance().select(RequestReplyProducer.class).get();
        await().until(() -> isRabbitMQConnectorAvailable(container));
        for (int i = 0; i < 10; i++) {
            producer.requestReply().request(Message.of(i)).subscribe().with(m -> replies.add(m.getPayload()));
        }
        await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(producer.requestReply().getPendingReplies()).isEmpty();
    }

    @Test
    public void testReplyFailureHandler() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducer.class, ReplyServerWithFailure.class, MyReplyFailureHandler.class);
        config(exchange, requestAddress)
                .with("mp.messaging.outgoing.request-reply." + REPLY_FAILURE_HANDLER_KEY, "my-reply-error")
                .write();
        container = weld.initialize();

        List<String> replies = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        RequestReplyProducer producer = container.getBeanManager().createInstance().select(RequestReplyProducer.class).get();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        for (int i = 0; i < 10; i++) {
            producer.requestReply().request(i)
                    .subscribe().with(replies::add, errors::add);
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(6));
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
    public void testReplyTimeout() {
        String exchange = "test-exchange";
        String requestAddress = "requests";
        weld.addBeanClasses(RequestReplyProducer.class, ReplyServerSlow.class);
        config(exchange, requestAddress)
                .with("mp.messaging.outgoing.request-reply." + REPLY_TIMEOUT_KEY, "1000")
                .write();
        container = weld.initialize();

        RequestReplyProducer producer = container.getBeanManager().createInstance().select(RequestReplyProducer.class).get();
        await().until(() -> isRabbitMQConnectorAvailable(container));

        producer.requestReply().request(1)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure().assertFailedWith(RabbitMQRequestReplyTimeoutException.class);
    }

    @ApplicationScoped
    public static class RequestReplyProducer {

        @Inject
        @Channel("request-reply")
        RabbitMQRequestReply<Integer, String> requestReply;

        public RabbitMQRequestReply<Integer, String> requestReply() {
            return requestReply;
        }
    }

    @ApplicationScoped
    public static class RequestReplyProducerWithConverter {

        @Inject
        @Channel("request-reply")
        RabbitMQRequestReply<Integer, byte[]> requestReply;

        public RabbitMQRequestReply<Integer, byte[]> requestReply() {
            return requestReply;
        }
    }

    @ApplicationScoped
    public static class RequestReplyProducerSecond {

        @Inject
        @Channel("request-reply")
        RabbitMQRequestReply<Integer, String> requestReply;

        @Inject
        @Channel("request-reply2")
        RabbitMQRequestReply<Integer, String> requestReply2;

        public RabbitMQRequestReply<Integer, String> requestReply() {
            return requestReply;
        }

        public RabbitMQRequestReply<Integer, String> requestReply2() {
            return requestReply2;
        }
    }

    @ApplicationScoped
    public static class ReplyServer {

        private static final Logger LOGGER = LoggerFactory.getLogger(ReplyServer.class);

        @Incoming("req")
        @Outgoing("rep")
        public Message<String> replier(Message<String> message) {
            LOGGER.info("Replying to " + message.getPayload());
            IncomingRabbitMQMetadata metadata = message.getMetadata(IncomingRabbitMQMetadata.class).get();
            String payload = message.getPayload();
            String response = payload + "";
            OutgoingRabbitMQMetadata outgoing = OutgoingRabbitMQMetadata.builder()
                    .withCorrelationId(metadata.getCorrelationId().get())
                    .withRoutingKey(metadata.getReplyTo().get()).build();
            return Message.of(response).addMetadata(outgoing);
        }
    }

    @ApplicationScoped
    public static class ReplyServerMultipleReplies {

        public static final int REPLIES = 10;

        @Incoming("req")
        @Outgoing("rep")
        Multi<Message<String>> process(Message<String> message) {
            if (message.getPayload() == null) {
                return null;
            }
            IncomingRabbitMQMetadata metadata = message.getMetadata(IncomingRabbitMQMetadata.class).get();
            String payload = message.getPayload();
            OutgoingRabbitMQMetadata outgoing = OutgoingRabbitMQMetadata.builder()
                    .withCorrelationId(metadata.getCorrelationId().get())
                    .withRoutingKey(metadata.getReplyTo().get()).build();
            return Multi.createFrom().emitter(multiEmitter -> {
                for (int i = 0; i < REPLIES; i++) {
                    multiEmitter.emit(Message.of(payload + ": " + i).addMetadata(outgoing));
                }
                multiEmitter.complete();
            });
        }
    }

    @ApplicationScoped
    public static class ReplyServerWithFailure {

        @Incoming("req")
        @Outgoing("rep")
        public Message<String> replier(Message<String> message) {
            IncomingRabbitMQMetadata metadata = message.getMetadata(IncomingRabbitMQMetadata.class).get();
            String payload = message.getPayload();
            Builder outgoing = OutgoingRabbitMQMetadata.builder()
                    .withCorrelationId(metadata.getCorrelationId().get())
                    .withRoutingKey(metadata.getReplyTo().get());
            if (Integer.parseInt(payload) % 3 == 0) {
                outgoing.withHeader("REPLY_ERROR", "Cannot reply to " + payload);
            }
            return Message.of(payload).addMetadata(outgoing.build());
        }
    }

    @ApplicationScoped
    @Identifier("my-reply-error")
    public static class MyReplyFailureHandler implements ReplyFailureHandler {

        @Override
        public Throwable handleReply(IncomingRabbitMQMessage<?> replyMessage) {
            String header = (String) replyMessage.getHeaders().get("REPLY_ERROR");
            if (header != null) {
                return new IllegalArgumentException(header);
            }
            return null;
        }
    }

    @ApplicationScoped
    public static class ReplyServerSlow {

        @Incoming("req")
        @Outgoing("rep")
        public Message<String> replier(Message<String> message) throws InterruptedException {
            IncomingRabbitMQMetadata metadata = message.getMetadata(IncomingRabbitMQMetadata.class).get();
            String payload = message.getPayload();
            String response = payload + "";
            OutgoingRabbitMQMetadata outgoing = OutgoingRabbitMQMetadata.builder()
                    .withCorrelationId(metadata.getCorrelationId().get())
                    .withRoutingKey(metadata.getReplyTo().get()).build();
            Thread.sleep(3000);
            return Message.of(response).addMetadata(outgoing);
        }
    }

}
