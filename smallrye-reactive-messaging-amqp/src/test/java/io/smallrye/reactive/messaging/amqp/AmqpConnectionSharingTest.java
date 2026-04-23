package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
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
import io.smallrye.reactive.messaging.amqp.reply.AmqpRequestReply;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpReceiver;

public class AmqpConnectionSharingTest extends AmqpBrokerTestBase {

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

    @Test
    public void testContainerIdIncomingAndOutgoing() {
        Weld weld = new Weld();
        weld.addBeanClasses(SharedConnectionProcessor.class);
        new MapBasedConfig()
                .with("mp.messaging.incoming.in-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in-channel.address", "shared-test-in")
                .with("mp.messaging.incoming.in-channel.host", host)
                .with("mp.messaging.incoming.in-channel.port", port)
                .with("mp.messaging.incoming.in-channel.username", username)
                .with("mp.messaging.incoming.in-channel.password", password)
                .with("mp.messaging.incoming.in-channel.tracing-enabled", false)
                .with("mp.messaging.incoming.in-channel.container-id", "my-shared-connection")
                .with("mp.messaging.outgoing.out-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out-channel.address", "shared-test-out")
                .with("mp.messaging.outgoing.out-channel.host", host)
                .with("mp.messaging.outgoing.out-channel.port", port)
                .with("mp.messaging.outgoing.out-channel.username", username)
                .with("mp.messaging.outgoing.out-channel.password", password)
                .with("mp.messaging.outgoing.out-channel.tracing-enabled", false)
                .with("mp.messaging.outgoing.out-channel.container-id", "my-shared-connection")
                .write();
        container = weld.initialize();

        AmqpConnector connector = container.getBeanManager().createInstance()
                .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> isAmqpConnectorReady(connector));

        assertThat(connector.getClients()).hasSize(1);
    }

    @Test
    public void testWithoutContainerIdSeparateConnections() {
        Weld weld = new Weld();
        weld.addBeanClasses(SharedConnectionProcessor.class);
        new MapBasedConfig()
                .with("mp.messaging.incoming.in-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in-channel.address", "no-share-in")
                .with("mp.messaging.incoming.in-channel.host", host)
                .with("mp.messaging.incoming.in-channel.port", port)
                .with("mp.messaging.incoming.in-channel.username", username)
                .with("mp.messaging.incoming.in-channel.password", password)
                .with("mp.messaging.incoming.in-channel.tracing-enabled", false)
                .with("mp.messaging.outgoing.out-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out-channel.address", "no-share-out")
                .with("mp.messaging.outgoing.out-channel.host", host)
                .with("mp.messaging.outgoing.out-channel.port", port)
                .with("mp.messaging.outgoing.out-channel.username", username)
                .with("mp.messaging.outgoing.out-channel.password", password)
                .with("mp.messaging.outgoing.out-channel.tracing-enabled", false)
                .write();
        container = weld.initialize();

        AmqpConnector connector = container.getBeanManager().createInstance()
                .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> isAmqpConnectorReady(connector));

        assertThat(connector.getClients()).hasSize(2);
    }

    @Test
    public void testSameContainerIdDifferentConfigFails() {
        Weld weld = new Weld();
        weld.addBeanClasses(SharedConnectionProcessor.class);
        new MapBasedConfig()
                .with("mp.messaging.incoming.in-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in-channel.address", "mismatch-in")
                .with("mp.messaging.incoming.in-channel.host", host)
                .with("mp.messaging.incoming.in-channel.port", port)
                .with("mp.messaging.incoming.in-channel.username", username)
                .with("mp.messaging.incoming.in-channel.password", password)
                .with("mp.messaging.incoming.in-channel.tracing-enabled", false)
                .with("mp.messaging.incoming.in-channel.container-id", "same-id")
                .with("mp.messaging.outgoing.out-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out-channel.address", "mismatch-out")
                .with("mp.messaging.outgoing.out-channel.host", host)
                .with("mp.messaging.outgoing.out-channel.port", port)
                .with("mp.messaging.outgoing.out-channel.username", "different-user")
                .with("mp.messaging.outgoing.out-channel.password", password)
                .with("mp.messaging.outgoing.out-channel.tracing-enabled", false)
                .with("mp.messaging.outgoing.out-channel.container-id", "same-id")
                .write();

        assertThatThrownBy(weld::initialize)
                .isInstanceOf(Exception.class);
    }

    @Test
    public void testContainerIdMultipleIncomingChannels() {
        Weld weld = new Weld();
        weld.addBeanClasses(DualIncomingBean.class);
        new MapBasedConfig()
                .with("mp.messaging.incoming.data1.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data1.address", "multi-share-1")
                .with("mp.messaging.incoming.data1.host", host)
                .with("mp.messaging.incoming.data1.port", port)
                .with("mp.messaging.incoming.data1.username", username)
                .with("mp.messaging.incoming.data1.password", password)
                .with("mp.messaging.incoming.data1.tracing-enabled", false)
                .with("mp.messaging.incoming.data1.container-id", "dual-incoming")
                .with("mp.messaging.incoming.data2.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data2.address", "multi-share-2")
                .with("mp.messaging.incoming.data2.host", host)
                .with("mp.messaging.incoming.data2.port", port)
                .with("mp.messaging.incoming.data2.username", username)
                .with("mp.messaging.incoming.data2.password", password)
                .with("mp.messaging.incoming.data2.tracing-enabled", false)
                .with("mp.messaging.incoming.data2.container-id", "dual-incoming")
                .write();
        container = weld.initialize();

        AmqpConnector connector = container.getBeanManager().createInstance()
                .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> isAmqpConnectorReady(connector));

        assertThat(connector.getClients()).hasSize(1);

        usage.produceTenIntegers("multi-share-1", () -> 1);
        usage.produceTenIntegers("multi-share-2", () -> 2);

        DualIncomingBean bean = container.getBeanManager().createInstance()
                .select(DualIncomingBean.class).get();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    assertThat(bean.list1()).hasSizeGreaterThanOrEqualTo(10);
                    assertThat(bean.list2()).hasSizeGreaterThanOrEqualTo(10);
                });
    }

    @Test
    public void testRequestReplySharesConnection() {
        startReplyServer();

        Weld weld = new Weld();
        weld.addBeanClasses(RequestReplyBean.class);
        new MapBasedConfig()
                .with("mp.messaging.outgoing.rr-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.rr-channel.address", "rr-share-requests")
                .with("mp.messaging.outgoing.rr-channel.host", host)
                .with("mp.messaging.outgoing.rr-channel.port", port)
                .with("mp.messaging.outgoing.rr-channel.username", username)
                .with("mp.messaging.outgoing.rr-channel.password", password)
                .with("mp.messaging.outgoing.rr-channel.tracing-enabled", false)
                .with("mp.messaging.outgoing.rr-channel.container-id", "rr-shared")
                .with("mp.messaging.outgoing.rr-channel.reply.address", "rr-share-replies")
                .write();
        container = weld.initialize();

        AmqpConnector connector = container.getBeanManager().createInstance()
                .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> isAmqpConnectorReady(connector));

        assertThat(connector.getClients()).hasSize(1);

        RequestReplyBean bean = container.getBeanManager().createInstance()
                .select(RequestReplyBean.class).get();

        List<String> replies = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 5; i++) {
            bean.requestReply().request(i).subscribe().with(replies::add);
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(replies).hasSize(5));
    }

    @Test
    public void testSharedConnectionExchangesMessages() {
        Weld weld = new Weld();
        weld.addBeanClasses(SharedConnectionProcessor.class);
        new MapBasedConfig()
                .with("mp.messaging.incoming.in-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in-channel.address", "shared-exchange-in")
                .with("mp.messaging.incoming.in-channel.host", host)
                .with("mp.messaging.incoming.in-channel.port", port)
                .with("mp.messaging.incoming.in-channel.username", username)
                .with("mp.messaging.incoming.in-channel.password", password)
                .with("mp.messaging.incoming.in-channel.tracing-enabled", false)
                .with("mp.messaging.incoming.in-channel.container-id", "exchange-test")
                .with("mp.messaging.outgoing.out-channel.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out-channel.address", "shared-exchange-out")
                .with("mp.messaging.outgoing.out-channel.host", host)
                .with("mp.messaging.outgoing.out-channel.port", port)
                .with("mp.messaging.outgoing.out-channel.username", username)
                .with("mp.messaging.outgoing.out-channel.password", password)
                .with("mp.messaging.outgoing.out-channel.tracing-enabled", false)
                .with("mp.messaging.outgoing.out-channel.container-id", "exchange-test")
                .write();
        container = weld.initialize();

        AmqpConnector connector = container.getBeanManager().createInstance()
                .select(AmqpConnector.class, ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
        await().until(() -> isAmqpConnectorReady(connector));

        assertThat(connector.getClients()).hasSize(1);

        List<String> received = new CopyOnWriteArrayList<>();
        usage.client.createReceiver("shared-exchange-out")
                .onItem().transformToMulti(AmqpReceiver::toMulti)
                .subscribe().with(m -> received.add(m.bodyAsString()));

        usage.produceTenIntegers("shared-exchange-in", () -> 42);

        SharedConnectionProcessor processor = container.getBeanManager().createInstance()
                .select(SharedConnectionProcessor.class).get();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(processor.processed()).hasSizeGreaterThanOrEqualTo(10));
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(received).hasSizeGreaterThanOrEqualTo(10));
    }

    private void startReplyServer() {
        usage.client.createReceiver("rr-share-requests")
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
    public static class SharedConnectionProcessor {

        private final List<String> processed = new CopyOnWriteArrayList<>();

        @Incoming("in-channel")
        @Outgoing("out-channel")
        public Message<String> process(Message<Integer> msg) {
            String value = "processed-" + msg.getPayload();
            processed.add(value);
            return msg.withPayload(value);
        }

        public List<String> processed() {
            return processed;
        }
    }

    @ApplicationScoped
    public static class DualIncomingBean {

        private final List<Integer> list1 = new CopyOnWriteArrayList<>();
        private final List<Integer> list2 = new CopyOnWriteArrayList<>();

        @Incoming("data1")
        public void consume1(int value) {
            list1.add(value);
        }

        @Incoming("data2")
        public void consume2(int value) {
            list2.add(value);
        }

        public List<Integer> list1() {
            return list1;
        }

        public List<Integer> list2() {
            return list2;
        }
    }

    @ApplicationScoped
    public static class RequestReplyBean {

        @Inject
        @Channel("rr-channel")
        AmqpRequestReply<Integer, String> requestReply;

        public AmqpRequestReply<Integer, String> requestReply() {
            return requestReply;
        }
    }

}
