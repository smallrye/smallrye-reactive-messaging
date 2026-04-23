package io.smallrye.reactive.messaging.amqp.cbs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.Target;
import org.apache.qpid.proton.message.Message;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.amqp.AmqpTestBase;
import io.smallrye.reactive.messaging.amqp.MockServer;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.SmallRyeConfigTestUtil;

public class AmqpCbsSetTokenTest extends AmqpTestBase {

    private WeldContainer container;
    private MockServer server;

    @BeforeEach
    public void resetTokenProvider() {
        MyCbsTokenProvider.invalid = false;
        MyCbsTokenProvider.failing = false;
        MyCbsTokenProvider.validityInSeconds = 2;
    }

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }

        if (server != null) {
            server.close();
        }
        // Release the config objects
        SmallRyeConfigTestUtil.releaseConfig();
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
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MyProducer {

        @Outgoing("messages-out")
        public Multi<String> produce() {
            return Multi.createFrom().items("Alice", "Bob", "Charlie");
        }
    }

    @ApplicationScoped
    static class MyCbsTokenProvider implements CbsTokenProvider {

        static boolean invalid = false;
        static boolean failing = false;
        static long validityInSeconds = 2;

        @Override
        public Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config) {
            if (invalid) {
                return Uni.createFrom().item(new DefaultCbsToken(null, "jwt", 0));
            }
            if (failing) {
                return Uni.createFrom().failure(new RuntimeException("Failed to retrieve token"));
            }
            return Uni.createFrom().item(new DefaultCbsToken("my-token-" + UUID.randomUUID(), "jwt", validityInSeconds));
        }
    }

    // ---- Happy path tests ----

    @Test
    public void testConnectionWithRefresh() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        AtomicReference<String> attachAddress = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createAcceptingCbsServer(tokenCache, messages, connectedLatch, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonSetTokenConfig(server.actualPort()).write();
        container = initializeContainer(weld);

        container.getBeanManager().createInstance().select(MyConsumer.class).get();

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());
        Object token = tokenCache.entrySet().stream().findFirst().get().getValue();
        await().pollDelay(3, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());
        Object refreshedToken = tokenCache.entrySet().stream().findFirst().get().getValue();
        assertThat(refreshedToken).isNotEqualTo(token);
    }

    @Test
    public void testOutgoingChannelWithCbs() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        AtomicReference<String> attachAddress = new AtomicReference<>();
        CountDownLatch messageLatch = new CountDownLatch(3);
        server = createAcceptingCbsServer(tokenCache, messages, messageLatch, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(MyProducer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.outgoing.messages-out.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.messages-out.host", "localhost")
                .put("mp.messaging.outgoing.messages-out.port", server.actualPort())
                .put("mp.messaging.outgoing.messages-out.cbs.enabled", true)
                .put("mp.messaging.outgoing.messages-out.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        await().atMost(5, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());
        assertThat(messageLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(messages).hasSize(3);
    }

    // ---- Error / rejection tests ----

    @Test
    public void testConnectionErrorWithoutCbs() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        AtomicReference<String> attachAddress = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createAcceptingCbsServer(tokenCache, messages, connectedLatch, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        container.getBeanManager().createInstance().select(MyConsumer.class).get();

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().pollDelay(3, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    @Test
    public void testInvalidToken() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        AtomicReference<String> attachAddress = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createAcceptingCbsServer(tokenCache, messages, connectedLatch, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        MyCbsTokenProvider.invalid = true;

        commonSetTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        container.getBeanManager().createInstance().select(MyConsumer.class).get();

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().pollDelay(3, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    @Test
    public void testTokenProviderFailure() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        AtomicReference<String> attachAddress = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createAcceptingCbsServer(tokenCache, messages, connectedLatch, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        MyCbsTokenProvider.failing = true;

        commonSetTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- CBS spec: set-token message format validation ----

    @Test
    public void testSetTokenMessageContainsRequiredProperties() throws Exception {
        AtomicReference<String> capturedSubject = new AtomicReference<>();
        AtomicReference<Map<String, Object>> capturedProps = new AtomicReference<>();
        AtomicReference<Object> capturedBody = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerCapturingSetTokenRequest(capturedSubject, capturedProps, capturedBody, connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonSetTokenConfig(server.actualPort()).write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();

        assertThat(capturedSubject.get()).isEqualTo("set-token");
        Map<String, Object> props = capturedProps.get();
        assertThat(props).containsKey("token-type");
        assertThat(props.get("token-type")).isEqualTo("jwt");
        assertThat(capturedBody.get()).isInstanceOf(String.class);
        assertThat((String) capturedBody.get()).startsWith("my-token-");
    }

    @Test
    public void testSetTokenDeliveryRejected() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerRejectingSetToken(connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonSetTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- CBS spec: custom CBS node ----

    @Test
    public void testCustomCbsNode() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        AtomicReference<String> attachAddress = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createAcceptingCbsServer(tokenCache, messages, connectedLatch, attachAddress, "$custom-cbs");

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());
    }

    @Test
    public void testDefaultCbsNodeWhenNotProvidedByServer() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        AtomicReference<String> attachAddress = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createAcceptingCbsServerWithoutCbsNodeProperty(tokenCache, messages, connectedLatch, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonSetTokenConfig(server.actualPort()).write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());
    }

    // ---- Token refresh failure triggers reconnection ----

    @Test
    public void testTokenRefreshFailureTriggersReconnection() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        AtomicReference<String> attachAddress = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createAcceptingCbsServer(tokenCache, messages, connectedLatch, attachAddress);

        MyCbsTokenProvider.validityInSeconds = 2;

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonSetTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.reconnect-attempts", 0)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 1)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());

        MyCbsTokenProvider.failing = true;

        // Refresh failure triggers closeCurrent(), readiness becomes false (holder nullified)
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- CBS Spec: AMQP_CBS_V1_0 capability ----

    @Test
    public void testCbsFailsWhenServerDoesNotAdvertiseCapability() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createServerWithoutCbsCapability(connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonSetTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    @Test
    public void testCbsFailsWhenServerHasNoCapabilities() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createServerWithNullCapabilities(connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonSetTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- CBS Spec: connection-scoped tokens ----

    @Test
    public void testIncomingAndOutgoingShareCbsAuthOnSameConnection() throws Exception {
        HashMap<String, Object> tokenCache = new HashMap<>();
        List<Message> messages = new CopyOnWriteArrayList<>();
        CountDownLatch messageLatch = new CountDownLatch(3);
        AtomicReference<String> attachAddress = new AtomicReference<>();
        server = createAcceptingCbsServer(tokenCache, messages, messageLatch, attachAddress);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyProducer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.container-id", "shared-container")
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false)
                .put("mp.messaging.outgoing.messages-out.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.messages-out.host", "localhost")
                .put("mp.messaging.outgoing.messages-out.port", server.actualPort())
                .put("mp.messaging.outgoing.messages-out.address", "messages-in")
                .put("mp.messaging.outgoing.messages-out.container-id", "shared-container")
                .put("mp.messaging.outgoing.messages-out.cbs.enabled", true)
                .put("mp.messaging.outgoing.messages-out.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        await().atMost(5, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());
        assertThat(tokenCache).hasSize(1);
    }

    // ---- Helper methods ----

    private MapBasedConfig commonSetTokenConfig(int port) {
        return new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", port)
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
    }

    MockServer createAcceptingCbsServer(Map<String, Object> tokenCache,
            List<Message> messages,
            CountDownLatch latch,
            AtomicReference<String> attachAddress) throws ExecutionException, InterruptedException {
        return createAcceptingCbsServer(tokenCache, messages, latch, attachAddress, "$cbs");
    }

    MockServer createAcceptingCbsServer(Map<String, Object> tokenCache, List<Message> messages,
            CountDownLatch latch,
            AtomicReference<String> attachAddress, String cbsAddress) throws ExecutionException, InterruptedException {
        return new MockServer(executionHolder.vertx().getDelegate(), serverConnection -> {
            serverConnection.setOfferedCapabilities(new Symbol[] { Symbol.valueOf("AMQP_CBS_V1_0") });
            serverConnection.setProperties(Map.of(Symbol.valueOf("cbs-node"), cbsAddress));
            serverConnection.openHandler(serverSender -> {
                serverConnection.closeHandler(x -> serverConnection.close());
                serverConnection.open();
            });

            serverConnection.sessionOpenHandler(serverSession -> {
                serverSession.closeHandler(x -> serverSession.close());
                serverSession.open();
            });

            serverConnection.senderOpenHandler(serverSender -> {
                Target remoteTarget = serverSender.getRemoteTarget();
                attachAddress.set(remoteTarget.getAddress());
                serverSender.setTarget(remoteTarget.copy());
                if (tokenCache.containsKey(serverConnection.getRemoteContainer())) {
                    serverSender.sendQueueDrainHandler(x -> {
                    }).open();
                } else {
                    serverConnection.setCondition(
                            new ErrorCondition(Symbol.valueOf("amqp:not-allowed"),
                                    "No token set for " + serverConnection.getRemoteContainer()));
                    serverConnection.close();
                }
            });

            serverConnection.receiverOpenHandler(serverReceiver -> {
                if (cbsAddress.equals(serverReceiver.getRemoteTarget().getAddress())) {
                    serverReceiver.handler((delivery, message) -> {
                        if (!"set-token".equals(message.getSubject())) {
                            delivery.disposition(new Rejected(), true);
                            return;
                        }
                        if (message.getApplicationProperties() == null
                                || message.getApplicationProperties().getValue() == null
                                || !message.getApplicationProperties().getValue().containsKey("token-type")) {
                            delivery.disposition(new Rejected(), true);
                            return;
                        }
                        if (message.getBody() == null || !(message.getBody() instanceof AmqpValue)) {
                            delivery.disposition(new Rejected(), true);
                            return;
                        }
                        AmqpValue token = (AmqpValue) message.getBody();
                        if (token.getValue() == null || !(token.getValue() instanceof String)) {
                            delivery.disposition(new Rejected(), true);
                            return;
                        }

                        tokenCache.put(serverConnection.getRemoteContainer(), token.getValue());
                        delivery.disposition(Accepted.getInstance(), true);
                        latch.countDown();
                    });
                } else if (tokenCache.containsKey(serverConnection.getRemoteContainer())) {
                    Target remoteTarget = serverReceiver.getRemoteTarget();
                    attachAddress.set(remoteTarget.getAddress());
                    serverReceiver.setTarget(remoteTarget.copy());

                    serverReceiver.handler((delivery, message) -> {
                        delivery.disposition(Accepted.getInstance(), true);
                        messages.add(message);
                        latch.countDown();
                    });
                } else {
                    serverConnection.close();
                    return;
                }

                serverReceiver.open();
            });
        });
    }

    MockServer createAcceptingCbsServerWithoutCbsNodeProperty(Map<String, Object> tokenCache, List<Message> messages,
            CountDownLatch latch,
            AtomicReference<String> attachAddress) throws ExecutionException, InterruptedException {
        return new MockServer(executionHolder.vertx().getDelegate(), serverConnection -> {
            serverConnection.setOfferedCapabilities(new Symbol[] { Symbol.valueOf("AMQP_CBS_V1_0") });
            serverConnection.openHandler(serverSender -> {
                serverConnection.closeHandler(x -> serverConnection.close());
                serverConnection.open();
            });

            serverConnection.sessionOpenHandler(serverSession -> {
                serverSession.closeHandler(x -> serverSession.close());
                serverSession.open();
            });

            serverConnection.senderOpenHandler(serverSender -> {
                Target remoteTarget = serverSender.getRemoteTarget();
                attachAddress.set(remoteTarget.getAddress());
                serverSender.setTarget(remoteTarget.copy());
                if (tokenCache.containsKey(serverConnection.getRemoteContainer())) {
                    serverSender.sendQueueDrainHandler(x -> {
                    }).open();
                } else {
                    serverConnection.setCondition(
                            new ErrorCondition(Symbol.valueOf("amqp:not-allowed"),
                                    "No token set"));
                    serverConnection.close();
                }
            });

            serverConnection.receiverOpenHandler(serverReceiver -> {
                if ("$cbs".equals(serverReceiver.getRemoteTarget().getAddress())) {
                    serverReceiver.handler((delivery, message) -> {
                        if (!"set-token".equals(message.getSubject())) {
                            delivery.disposition(new Rejected(), true);
                            return;
                        }
                        if (message.getBody() instanceof AmqpValue token
                                && token.getValue() instanceof String tokenValue) {
                            tokenCache.put(serverConnection.getRemoteContainer(), tokenValue);
                            delivery.disposition(Accepted.getInstance(), true);
                            latch.countDown();
                        } else {
                            delivery.disposition(new Rejected(), true);
                        }
                    });
                } else if (tokenCache.containsKey(serverConnection.getRemoteContainer())) {
                    Target remoteTarget = serverReceiver.getRemoteTarget();
                    attachAddress.set(remoteTarget.getAddress());
                    serverReceiver.setTarget(remoteTarget.copy());
                    serverReceiver.handler((delivery, message) -> {
                        delivery.disposition(Accepted.getInstance(), true);
                        messages.add(message);
                        latch.countDown();
                    });
                } else {
                    serverConnection.close();
                    return;
                }
                serverReceiver.open();
            });
        });
    }

    private MockServer createCbsServerCapturingSetTokenRequest(AtomicReference<String> capturedSubject,
            AtomicReference<Map<String, Object>> capturedProps,
            AtomicReference<Object> capturedBody,
            CountDownLatch latch) throws ExecutionException, InterruptedException {
        AtomicBoolean tokenAccepted = new AtomicBoolean(false);
        return new MockServer(executionHolder.vertx().getDelegate(), serverConnection -> {
            serverConnection.setOfferedCapabilities(new Symbol[] { Symbol.valueOf("AMQP_CBS_V1_0") });
            serverConnection.setProperties(Map.of(Symbol.valueOf("cbs-node"), "$cbs"));
            serverConnection.openHandler(x -> {
                serverConnection.closeHandler(cx -> serverConnection.close());
                serverConnection.open();
            });
            serverConnection.sessionOpenHandler(s -> {
                s.closeHandler(x -> s.close());
                s.open();
            });
            serverConnection.senderOpenHandler(s -> {
                Target remoteTarget = s.getRemoteTarget();
                s.setTarget(remoteTarget.copy());
                if (tokenAccepted.get()) {
                    s.sendQueueDrainHandler(x -> {
                    }).open();
                } else {
                    serverConnection.setCondition(new ErrorCondition(Symbol.valueOf("amqp:not-allowed"), "No token"));
                    serverConnection.close();
                }
            });
            serverConnection.receiverOpenHandler(r -> {
                if ("$cbs".equals(r.getRemoteTarget().getAddress())) {
                    r.handler((delivery, message) -> {
                        capturedSubject.set(message.getSubject());
                        if (message.getApplicationProperties() != null) {
                            capturedProps.set(new HashMap<>(message.getApplicationProperties().getValue()));
                        }
                        if (message.getBody() instanceof AmqpValue) {
                            capturedBody.set(((AmqpValue) message.getBody()).getValue());
                        }
                        tokenAccepted.set(true);
                        delivery.disposition(Accepted.getInstance(), true);
                        latch.countDown();
                    });
                } else if (tokenAccepted.get()) {
                    Target remoteTarget = r.getRemoteTarget();
                    r.setTarget(remoteTarget.copy());
                    r.handler((delivery, message) -> delivery.disposition(Accepted.getInstance(), true));
                } else {
                    serverConnection.close();
                    return;
                }
                r.open();
            });
        });
    }

    private MockServer createCbsServerRejectingSetToken(CountDownLatch latch) throws ExecutionException, InterruptedException {
        return new MockServer(executionHolder.vertx().getDelegate(), serverConnection -> {
            serverConnection.setOfferedCapabilities(new Symbol[] { Symbol.valueOf("AMQP_CBS_V1_0") });
            serverConnection.setProperties(Map.of(Symbol.valueOf("cbs-node"), "$cbs"));
            serverConnection.openHandler(x -> {
                serverConnection.closeHandler(cx -> serverConnection.close());
                serverConnection.open();
            });
            serverConnection.sessionOpenHandler(s -> {
                s.closeHandler(x -> s.close());
                s.open();
            });
            serverConnection.senderOpenHandler(s -> {
                serverConnection.setCondition(new ErrorCondition(Symbol.valueOf("amqp:not-allowed"), "No token"));
                serverConnection.close();
            });
            serverConnection.receiverOpenHandler(r -> {
                if ("$cbs".equals(r.getRemoteTarget().getAddress())) {
                    r.handler((delivery, message) -> {
                        delivery.disposition(new Rejected(), true);
                    });
                } else {
                    serverConnection.close();
                    return;
                }
                r.open();
            });
        });
    }

    private MockServer createServerWithoutCbsCapability(CountDownLatch latch)
            throws ExecutionException, InterruptedException {
        return new MockServer(executionHolder.vertx().getDelegate(), serverConnection -> {
            serverConnection.setOfferedCapabilities(new Symbol[] { Symbol.valueOf("ANONYMOUS-RELAY") });
            serverConnection.openHandler(x -> {
                serverConnection.closeHandler(cx -> serverConnection.close());
                serverConnection.open();
            });
            serverConnection.sessionOpenHandler(s -> {
                s.closeHandler(x -> s.close());
                s.open();
            });
            serverConnection.receiverOpenHandler(r -> r.open());
            serverConnection.senderOpenHandler(s -> s.open());
        });
    }

    private MockServer createServerWithNullCapabilities(CountDownLatch latch)
            throws ExecutionException, InterruptedException {
        return new MockServer(executionHolder.vertx().getDelegate(), serverConnection -> {
            serverConnection.openHandler(x -> {
                serverConnection.closeHandler(cx -> serverConnection.close());
                serverConnection.open();
            });
            serverConnection.sessionOpenHandler(s -> {
                s.closeHandler(x -> s.close());
                s.open();
            });
            serverConnection.receiverOpenHandler(r -> r.open());
            serverConnection.senderOpenHandler(s -> s.open());
        });
    }
}
