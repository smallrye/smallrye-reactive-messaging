package io.smallrye.reactive.messaging.amqp.cbs;

import static io.smallrye.reactive.messaging.amqp.cbs.AzureKeyHelper.getSASToken;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.amqp.AmqpTestBase;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;
import io.smallrye.reactive.messaging.amqp.reply.AmqpRequestReply;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@Disabled
public class AzureCbsTokenTest extends AmqpTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }
    }

    public boolean isAmqpConnectorReady() {
        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        return connector.getReadiness().isOk();
    }

    public boolean isAmqpConnectorAlive() {
        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        return connector.getLiveness().isOk();
    }

    @ApplicationScoped
    public static class MyConsumer {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("messages-in")
        public void getPeople(String s) {
            list.add(s);
            System.out.println("Received: " + s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MyProducer {

        @Outgoing("messages-out")
        public Multi<String> producePeople() {
            return Multi.createFrom().items("Alice", "Bob", "Charlie");
        }

    }

    @ApplicationScoped
    public static class RequestReplyRequester {

        @Inject
        @Channel("request-reply")
        AmqpRequestReply<String, String> requestReply;

        public AmqpRequestReply<String, String> requestReply() {
            return requestReply;
        }
    }

    @ApplicationScoped
    public static class RequestReplyResponder {

        @Incoming("requests")
        @Outgoing("replies")
        public Message<String> process(AmqpMessage<String> request) {
            return org.eclipse.microprofile.reactive.messaging.Message.of("reply:" + request.getPayload())
                    .addMetadata(OutgoingAmqpMetadata.builder()
                            .withCorrelationId(request.getMessageId().toString())
                            .withAddress(request.unwrap().getReplyTo())
                            .build());
        }
    }

    private static final String AZURE_HOST = "cbs-test.servicebus.windows.net";

    @ApplicationScoped
    static class AzureTokenProvider implements CbsTokenProvider {

        @Override
        public Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config) {
            String key = config instanceof AmqpConnectorIncomingConfiguration ? System.getenv("TEST_LISTEN_KEY")
                    : System.getenv("TEST_SEND_KEY");
            String keyName = config instanceof AmqpConnectorIncomingConfiguration ? "in_listen_key" : "in_send_key";
            return Uni.createFrom().item(
                    new DefaultCbsToken(
                            getSASToken(AZURE_HOST, keyName, key), "servicebus.windows.net:sastoken", 3600));
        }
    }

    @ApplicationScoped
    static class ShortLivedAzureTokenProvider implements CbsTokenProvider {

        static final AtomicLong tokenCount = new AtomicLong(0);
        static long sasValiditySeconds = 120;
        static long cbsValiditySeconds = 30;

        @Override
        public Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config) {
            tokenCount.incrementAndGet();
            String key = config instanceof AmqpConnectorIncomingConfiguration ? System.getenv("TEST_LISTEN_KEY")
                    : System.getenv("TEST_SEND_KEY");
            String keyName = config instanceof AmqpConnectorIncomingConfiguration ? "in_listen_key" : "in_send_key";
            return Uni.createFrom().item(
                    new DefaultCbsToken(
                            getSASToken(AZURE_HOST, keyName, key, sasValiditySeconds),
                            "servicebus.windows.net:sastoken", cbsValiditySeconds));
        }
    }

    @ApplicationScoped
    static class SwappedKeysTokenProvider implements CbsTokenProvider {

        @Override
        public Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config) {
            // Intentionally swapped: use Send key for listening, Listen key for sending
            String key = config instanceof AmqpConnectorIncomingConfiguration ? System.getenv("TEST_SEND_KEY")
                    : System.getenv("TEST_LISTEN_KEY");
            String keyName = config instanceof AmqpConnectorIncomingConfiguration ? "in_send_key" : "in_listen_key";
            return Uni.createFrom().item(
                    new DefaultCbsToken(
                            getSASToken(AZURE_HOST, keyName, key),
                            "servicebus.windows.net:sastoken", 3600));
        }
    }

    @ApplicationScoped
    static class InvalidKeyTokenProvider implements CbsTokenProvider {

        @Override
        public Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config) {
            return Uni.createFrom().item(
                    new DefaultCbsToken(
                            getSASToken(AZURE_HOST, "invalid_policy", "aW52YWxpZGtleQ=="),
                            "servicebus.windows.net:sastoken", 3600));
        }
    }

    @ApplicationScoped
    static class FailoverTokenProvider implements CbsTokenProvider {

        static final AtomicBoolean failing = new AtomicBoolean(false);

        @Override
        public Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config) {
            if (failing.get()) {
                return Uni.createFrom().failure(new RuntimeException("Simulated token retrieval failure"));
            }
            String key = config instanceof AmqpConnectorIncomingConfiguration ? System.getenv("TEST_LISTEN_KEY")
                    : System.getenv("TEST_SEND_KEY");
            String keyName = config instanceof AmqpConnectorIncomingConfiguration ? "in_listen_key" : "in_send_key";
            return Uni.createFrom().item(
                    new DefaultCbsToken(
                            getSASToken(AZURE_HOST, keyName, key, 120),
                            "servicebus.windows.net:sastoken", 10));
        }
    }

    // ---- Helper methods ----

    private MapBasedConfig incomingConfig() {
        return new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", AZURE_HOST)
                .put("mp.messaging.incoming.messages-in.port", 5671)
                .put("mp.messaging.incoming.messages-in.use-ssl", true)
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
    }

    private MapBasedConfig outgoingConfig() {
        return new MapBasedConfig()
                .put("mp.messaging.outgoing.messages-out.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.messages-out.host", AZURE_HOST)
                .put("mp.messaging.outgoing.messages-out.port", 5671)
                .put("mp.messaging.outgoing.messages-out.address", "messages-in")
                .put("mp.messaging.outgoing.messages-out.use-ssl", true)
                .put("mp.messaging.outgoing.messages-out.cbs.enabled", true)
                .put("mp.messaging.outgoing.messages-out.cbs.exchange", "put-token")
                .put("mp.messaging.outgoing.messages-out.tracing-enabled", false);
    }

    private MapBasedConfig incomingAndOutgoingConfig() {
        MapBasedConfig config = incomingConfig();
        config.putAll(outgoingConfig());
        return config;
    }

    // ---- 1. Happy path: send and receive ----

    @Test
    public void testSendAndReceive() {
        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyProducer.class);
        weld.addBeanClass(AzureTokenProvider.class);

        incomingAndOutgoingConfig().write();

        container = initializeContainer(weld);

        MyConsumer myConsumer = container.getBeanManager().createInstance().select(MyConsumer.class).get();

        await().until(() -> isAmqpConnectorAlive() && isAmqpConnectorReady());
        await().untilAsserted(() -> Assertions.assertThat(myConsumer.list()).hasSizeGreaterThanOrEqualTo(3)
                .contains("Alice", "Bob", "Charlie"));
    }

    // ---- Set-token flow: not supported by Azure Service Bus ----

    @Test
    public void testSetTokenRejectedByAzure() {
        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(AzureTokenProvider.class);

        incomingConfig()
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "set-token")
                .put("mp.messaging.incoming.messages-in.reconnect-attempts", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();

        container = initializeContainer(weld);

        await().atMost(30, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- 2. Token refresh with short-lived SAS tokens ----

    @Test
    public void testTokenRefreshWithShortLivedToken() {
        ShortLivedAzureTokenProvider.tokenCount.set(0);
        ShortLivedAzureTokenProvider.sasValiditySeconds = 120;
        ShortLivedAzureTokenProvider.cbsValiditySeconds = 30;

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(ShortLivedAzureTokenProvider.class);

        incomingConfig().write();

        container = initializeContainer(weld);

        await().atMost(10, TimeUnit.SECONDS).until(() -> isAmqpConnectorAlive() && isAmqpConnectorReady());
        long initialCount = ShortLivedAzureTokenProvider.tokenCount.get();
        Assertions.assertThat(initialCount).isGreaterThanOrEqualTo(1);

        // Wait for at least one token refresh cycle
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(ShortLivedAzureTokenProvider.tokenCount.get())
                        .isGreaterThan(initialCount));

        // Connector should still be healthy after refresh
        Assertions.assertThat(isAmqpConnectorAlive()).isTrue();
        Assertions.assertThat(isAmqpConnectorReady()).isTrue();
    }

    // ---- 3. Wrong permissions: swapped keys ----

    @Test
    public void testWrongPermissionsWithSwappedKeys() {
        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(SwappedKeysTokenProvider.class);

        incomingConfig()
                .put("mp.messaging.incoming.messages-in.reconnect-attempts", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();

        container = initializeContainer(weld);

        await().atMost(30, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- 4. Invalid/expired key ----

    @Test
    public void testInvalidKey() {
        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(InvalidKeyTokenProvider.class);

        incomingConfig()
                .put("mp.messaging.incoming.messages-in.reconnect-attempts", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();

        container = weld.initialize();

        await().atMost(30, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- 5. Reconnection after refresh failure ----

    @Test
    public void testReconnectionAfterRefreshFailure() {
        FailoverTokenProvider.failing.set(false);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(FailoverTokenProvider.class);

        incomingConfig()
                .put("mp.messaging.incoming.messages-in.reconnect-attempts", 0)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 1)
                .write();

        container = weld.initialize();

        await().atMost(15, TimeUnit.SECONDS).until(() -> isAmqpConnectorAlive() && isAmqpConnectorReady());

        // Simulate token provider failure for subsequent refreshes
        FailoverTokenProvider.failing.set(true);

        await().atMost(30, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- 6. Health probes reflect CBS auth state ----

    @Test
    public void testHealthProbesReflectCbsAuthState() {
        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(AzureTokenProvider.class);

        incomingConfig().write();

        container = initializeContainer(weld);

        // Readiness and liveness should become true after successful CBS auth
        await().atMost(15, TimeUnit.SECONDS).until(() -> isAmqpConnectorAlive() && isAmqpConnectorReady());
    }

    @Test
    public void testHealthProbesUnhealthyWithInvalidAuth() {
        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(InvalidKeyTokenProvider.class);

        incomingConfig()
                .put("mp.messaging.incoming.messages-in.reconnect-attempts", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();

        container = initializeContainer(weld);

        // Readiness should remain false with invalid credentials
        await().atMost(30, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- 7. Request-reply with CBS ----

    @Test
    public void testRequestReplyWithCbs() {
        Weld weld = new Weld();
        weld.addBeanClass(RequestReplyRequester.class);
        weld.addBeanClass(RequestReplyResponder.class);
        weld.addBeanClass(AzureTokenProvider.class);

        MapBasedConfig config = new MapBasedConfig()
                // Request-reply outgoing channel (sends requests to "requests" queue)
                .put("mp.messaging.outgoing.request-reply.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.request-reply.host", AZURE_HOST)
                .put("mp.messaging.outgoing.request-reply.port", 5671)
                .put("mp.messaging.outgoing.request-reply.address", "requests")
                .put("mp.messaging.outgoing.request-reply.use-ssl", true)
                .put("mp.messaging.outgoing.request-reply.cbs.enabled", true)
                .put("mp.messaging.outgoing.request-reply.cbs.exchange", "put-token")
                .put("mp.messaging.outgoing.request-reply.tracing-enabled", false)
                .put("mp.messaging.outgoing.request-reply.reply.address", "replies")
                .put("mp.messaging.outgoing.request-reply.reply.cbs.enabled", true)
                .put("mp.messaging.outgoing.request-reply.reply.cbs.exchange", "put-token")
                .put("mp.messaging.outgoing.request-reply.reply-timeout", 10000)
                // Responder incoming channel (consumes from "requests" queue)
                .put("mp.messaging.incoming.requests.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.requests.host", AZURE_HOST)
                .put("mp.messaging.incoming.requests.port", 5671)
                .put("mp.messaging.incoming.requests.use-ssl", true)
                .put("mp.messaging.incoming.requests.cbs.enabled", true)
                .put("mp.messaging.incoming.requests.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.requests.tracing-enabled", false)
                // Responder outgoing channel (sends replies back)
                .put("mp.messaging.outgoing.replies.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.replies.host", AZURE_HOST)
                .put("mp.messaging.outgoing.replies.port", 5671)
                .put("mp.messaging.outgoing.replies.use-ssl", true)
                .put("mp.messaging.outgoing.replies.cbs.enabled", true)
                .put("mp.messaging.outgoing.replies.cbs.exchange", "put-token")
                .put("mp.messaging.outgoing.replies.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        await().atMost(15, TimeUnit.SECONDS).until(() -> isAmqpConnectorAlive() && isAmqpConnectorReady());

        RequestReplyRequester requester = container.getBeanManager().createInstance()
                .select(RequestReplyRequester.class).get();

        List<String> replies = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 3; i++) {
            requester.requestReply().request("msg-" + i)
                    .subscribe().with(replies::add);
        }

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> Assertions.assertThat(replies).hasSize(3)
                        .containsExactlyInAnyOrder("reply:msg-0", "reply:msg-1", "reply:msg-2"));
    }

}
