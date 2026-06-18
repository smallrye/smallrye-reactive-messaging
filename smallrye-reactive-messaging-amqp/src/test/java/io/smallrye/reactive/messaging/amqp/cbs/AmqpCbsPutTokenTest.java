package io.smallrye.reactive.messaging.amqp.cbs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;
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
import io.vertx.proton.ProtonSender;

public class AmqpCbsPutTokenTest extends AmqpTestBase {

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

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.cbs-audience", "my-audience")
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

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
                .put("mp.messaging.outgoing.messages-out.cbs.exchange", "put-token")
                .put("mp.messaging.outgoing.messages-out.cbs-audience", "my-audience")
                .put("mp.messaging.outgoing.messages-out.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        await().atMost(5, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());
        assertThat(messageLatch.await(5, TimeUnit.SECONDS)).isTrue();
        await().untilAsserted(() -> assertThat(messages).hasSize(3));
    }

    @Test
    public void testAudienceDerivedFromAddress() throws Exception {
        AtomicReference<String> receivedAudience = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerCapturingAudience(receivedAudience, connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.address", "my-queue")
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(receivedAudience.get()).isEqualTo("amqp://localhost/my-queue");
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

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isFalse();
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

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.cbs-audience", "my-audience")
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

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

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.cbs-audience", "my-audience")
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- CBS spec status code tests ----

    @Test
    public void testPutTokenStatus200Ok() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithStatusCode(200, "OK", connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort()).write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS).until(this::isAmqpConnectorReady);
    }

    @Test
    public void testPutTokenStatus202Accepted() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithStatusCode(202, "Accepted", connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort()).write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS).until(this::isAmqpConnectorReady);
    }

    @Test
    public void testPutTokenStatus401Unauthorized() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithStatusCode(401, "Unauthorized", connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    @Test
    public void testPutTokenStatus403Forbidden() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithStatusCode(403, "Forbidden", connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    @Test
    public void testPutTokenStatus404EntityNotFound() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithStatusCode(404, "The messaging entity 'test-queue' could not be found",
                connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    @Test
    public void testPutTokenStatus400BadRequest() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithStatusCode(400, "Bad Request", connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    @Test
    public void testPutTokenStatusCodeAsLong() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithStatusCodeAsLong(202L, "Accepted", connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort()).write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS).until(this::isAmqpConnectorReady);
    }

    @Test
    public void testPutTokenLegacyStatusCodeProperties() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithLegacyStatusCode(202, "Accepted", connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort()).write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS).until(this::isAmqpConnectorReady);
    }

    @Test
    public void testPutTokenNoStatusCodeInResponse() throws Exception {
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerWithNoStatusCode(connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 2)
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(1, TimeUnit.SECONDS)).isFalse();
        await().atMost(10, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady());
    }

    // ---- CBS spec: put-token message format validation ----

    @Test
    public void testPutTokenMessageContainsRequiredProperties() throws Exception {
        AtomicReference<Map<String, Object>> capturedProps = new AtomicReference<>();
        AtomicReference<Object> capturedBody = new AtomicReference<>();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        server = createCbsServerCapturingRequest(capturedProps, capturedBody, connectedLatch);

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);
        weld.addBeanClass(MyCbsTokenProvider.class);

        commonPutTokenConfig(server.actualPort())
                .put("mp.messaging.incoming.messages-in.cbs.audience", "my-audience")
                .write();
        container = initializeContainer(weld);

        assertThat(connectedLatch.await(3, TimeUnit.SECONDS)).isTrue();

        Map<String, Object> props = capturedProps.get();
        assertThat(props).containsKey("operation");
        assertThat(props.get("operation")).isEqualTo("put-token");
        assertThat(props).containsKey("type");
        assertThat(props.get("type")).isEqualTo("jwt");
        assertThat(props).containsKey("name");
        assertThat(props.get("name")).isEqualTo("my-audience");
        assertThat(props).containsKey("expiration");
        assertThat(capturedBody.get()).isInstanceOf(String.class);
        assertThat((String) capturedBody.get()).startsWith("my-token-");
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
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.cbs-audience", "my-audience")
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

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

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.cbs-audience", "my-audience")
                .put("mp.messaging.incoming.messages-in.reconnect-attempts", 0)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-interval", 1)
                .put("mp.messaging.incoming.messages-in.retry-on-fail-attempts", 1)
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

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

        commonPutTokenConfig(server.actualPort())
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

        commonPutTokenConfig(server.actualPort())
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
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.cbs-audience", "my-audience")
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false)
                .put("mp.messaging.outgoing.messages-out.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.outgoing.messages-out.host", "localhost")
                .put("mp.messaging.outgoing.messages-out.port", server.actualPort())
                .put("mp.messaging.outgoing.messages-out.address", "messages-in")
                .put("mp.messaging.outgoing.messages-out.container-id", "shared-container")
                .put("mp.messaging.outgoing.messages-out.cbs.enabled", true)
                .put("mp.messaging.outgoing.messages-out.cbs.exchange", "put-token")
                .put("mp.messaging.outgoing.messages-out.cbs-audience", "my-audience")
                .put("mp.messaging.outgoing.messages-out.tracing-enabled", false);
        config.write();

        container = initializeContainer(weld);

        await().atMost(5, TimeUnit.SECONDS).until(() -> !tokenCache.isEmpty());
        // Both channels share one connection, so only one token exchange should have occurred
        assertThat(tokenCache).hasSize(1);
    }

    // ---- Helper methods ----

    private MapBasedConfig commonPutTokenConfig(int port) {
        return new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", "localhost")
                .put("mp.messaging.incoming.messages-in.port", port)
                .put("mp.messaging.incoming.messages-in.cbs.enabled", true)
                .put("mp.messaging.incoming.messages-in.cbs.exchange", "put-token")
                .put("mp.messaging.incoming.messages-in.cbs-audience", "my-audience")
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

            Map<String, ProtonSender> senders = new HashMap<>();

            serverConnection.senderOpenHandler(serverSender -> {
                if (serverSender.getRemoteSource().getAddress().startsWith(cbsAddress)) {
                    serverSender.open();
                    senders.put(serverSender.getRemoteSource().getAddress(), serverSender);
                } else if (tokenCache.containsKey(serverConnection.getRemoteContainer())) {
                    serverSender.open();
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
                        Map<String, Object> appProps = message.getApplicationProperties().getValue();
                        if (appProps == null) {
                            delivery.disposition(new Rejected(), true);
                            return;
                        }
                        String operation = (String) appProps.get("operation");
                        if (operation != null && !operation.equals("put-token")) {
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

                        Message response = Message.Factory.create();
                        response.setCorrelationId(message.getMessageId());
                        response.setApplicationProperties(new org.apache.qpid.proton.amqp.messaging.ApplicationProperties(
                                java.util.Map.of("status-code", 202, "status-description", "Accepted")));
                        response.setBody(new AmqpValue("MockTokenAccepted"));

                        tokenCache.put(serverConnection.getRemoteContainer(), token.getValue());
                        delivery.disposition(Accepted.getInstance(), true);
                        ProtonSender sender = senders.get(message.getReplyTo());
                        if (sender != null) {
                            sender.send(response);
                        }
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

    private MockServer createCbsServerWithStatusCode(int statusCode, String statusDescription,
            CountDownLatch latch) throws ExecutionException, InterruptedException {
        return createCbsServerRespondingWith("statusCode", statusCode, "statusDescription", statusDescription, latch);
    }

    private MockServer createCbsServerWithStatusCodeAsLong(long statusCode, String statusDescription,
            CountDownLatch latch) throws ExecutionException, InterruptedException {
        return createCbsServerRespondingWith("statusCode", statusCode, "statusDescription", statusDescription, latch);
    }

    private MockServer createCbsServerWithLegacyStatusCode(int statusCode, String statusDescription,
            CountDownLatch latch) throws ExecutionException, InterruptedException {
        return createCbsServerRespondingWith("status-code", statusCode, "status-description", statusDescription, latch);
    }

    private MockServer createCbsServerWithNoStatusCode(CountDownLatch latch) throws ExecutionException, InterruptedException {
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
            Map<String, ProtonSender> senders = new HashMap<>();
            serverConnection.senderOpenHandler(s -> {
                if (s.getRemoteSource().getAddress().startsWith("$cbs")) {
                    s.open();
                    senders.put(s.getRemoteSource().getAddress(), s);
                } else {
                    s.open();
                }
            });
            serverConnection.receiverOpenHandler(r -> {
                if ("$cbs".equals(r.getRemoteTarget().getAddress())) {
                    r.handler((delivery, message) -> {
                        Message response = Message.Factory.create();
                        response.setCorrelationId(message.getMessageId());
                        response.setBody(new AmqpValue("no-status"));
                        delivery.disposition(Accepted.getInstance(), true);
                        ProtonSender sender = senders.get(message.getReplyTo());
                        if (sender != null) {
                            sender.send(response);
                        }
                    });
                }
                r.open();
            });
        });
    }

    private MockServer createCbsServerRespondingWith(String statusCodeKey, Object statusCode,
            String statusDescKey, String statusDescription,
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
            Map<String, ProtonSender> senders = new HashMap<>();
            serverConnection.senderOpenHandler(s -> {
                if (s.getRemoteSource().getAddress().startsWith("$cbs")) {
                    s.open();
                    senders.put(s.getRemoteSource().getAddress(), s);
                } else if (tokenAccepted.get()) {
                    s.open();
                } else {
                    serverConnection.setCondition(new ErrorCondition(Symbol.valueOf("amqp:not-allowed"), "No token"));
                    serverConnection.close();
                }
            });
            serverConnection.receiverOpenHandler(r -> {
                if ("$cbs".equals(r.getRemoteTarget().getAddress())) {
                    r.handler((delivery, message) -> {
                        Message response = Message.Factory.create();
                        response.setCorrelationId(message.getMessageId());
                        response.setApplicationProperties(
                                new org.apache.qpid.proton.amqp.messaging.ApplicationProperties(
                                        java.util.Map.of(statusCodeKey, statusCode,
                                                statusDescKey, statusDescription)));
                        response.setBody(new AmqpValue("response"));
                        delivery.disposition(Accepted.getInstance(), true);
                        ProtonSender sender = senders.get(message.getReplyTo());
                        if (sender != null) {
                            sender.send(response);
                        }
                        int code = statusCode instanceof Number ? ((Number) statusCode).intValue() : -1;
                        if (code == 200 || code == 202) {
                            tokenAccepted.set(true);
                            latch.countDown();
                        }
                    });
                } else if (tokenAccepted.get()) {
                    Target remoteTarget = r.getRemoteTarget();
                    r.setTarget(remoteTarget.copy());
                    r.handler((delivery, message) -> {
                        delivery.disposition(Accepted.getInstance(), true);
                        latch.countDown();
                    });
                } else {
                    serverConnection.close();
                    return;
                }
                r.open();
            });
        });
    }

    private MockServer createCbsServerCapturingAudience(AtomicReference<String> receivedAudience,
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
            Map<String, ProtonSender> senders = new HashMap<>();
            serverConnection.senderOpenHandler(s -> {
                if (s.getRemoteSource().getAddress().startsWith("$cbs")) {
                    s.open();
                    senders.put(s.getRemoteSource().getAddress(), s);
                } else if (tokenAccepted.get()) {
                    s.open();
                } else {
                    serverConnection.setCondition(new ErrorCondition(Symbol.valueOf("amqp:not-allowed"), "No token"));
                    serverConnection.close();
                }
            });
            serverConnection.receiverOpenHandler(r -> {
                if ("$cbs".equals(r.getRemoteTarget().getAddress())) {
                    r.handler((delivery, message) -> {
                        Map<String, Object> appProps = message.getApplicationProperties().getValue();
                        receivedAudience.set((String) appProps.get("name"));

                        Message response = Message.Factory.create();
                        response.setCorrelationId(message.getMessageId());
                        response.setApplicationProperties(
                                new org.apache.qpid.proton.amqp.messaging.ApplicationProperties(
                                        java.util.Map.of("statusCode", 202, "statusDescription", "Accepted")));
                        response.setBody(new AmqpValue("ok"));
                        delivery.disposition(Accepted.getInstance(), true);
                        tokenAccepted.set(true);
                        ProtonSender sender = senders.get(message.getReplyTo());
                        if (sender != null) {
                            sender.send(response);
                        }
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

    private MockServer createCbsServerCapturingRequest(AtomicReference<Map<String, Object>> capturedProps,
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
            Map<String, ProtonSender> senders = new HashMap<>();
            serverConnection.senderOpenHandler(s -> {
                if (s.getRemoteSource().getAddress().startsWith("$cbs")) {
                    s.open();
                    senders.put(s.getRemoteSource().getAddress(), s);
                } else if (tokenAccepted.get()) {
                    s.open();
                } else {
                    serverConnection.setCondition(new ErrorCondition(Symbol.valueOf("amqp:not-allowed"), "No token"));
                    serverConnection.close();
                }
            });
            serverConnection.receiverOpenHandler(r -> {
                if ("$cbs".equals(r.getRemoteTarget().getAddress())) {
                    r.handler((delivery, message) -> {
                        capturedProps.set(new HashMap<>(message.getApplicationProperties().getValue()));
                        if (message.getBody() instanceof AmqpValue) {
                            capturedBody.set(((AmqpValue) message.getBody()).getValue());
                        }

                        Message response = Message.Factory.create();
                        response.setCorrelationId(message.getMessageId());
                        response.setApplicationProperties(
                                new org.apache.qpid.proton.amqp.messaging.ApplicationProperties(
                                        java.util.Map.of("statusCode", 202, "statusDescription", "Accepted")));
                        response.setBody(new AmqpValue("ok"));
                        delivery.disposition(Accepted.getInstance(), true);
                        tokenAccepted.set(true);
                        ProtonSender sender = senders.get(message.getReplyTo());
                        if (sender != null) {
                            sender.send(response);
                        }
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
