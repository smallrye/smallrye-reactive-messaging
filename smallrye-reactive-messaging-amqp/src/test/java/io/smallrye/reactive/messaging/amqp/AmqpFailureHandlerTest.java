package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.DeliveryState.DeliveryStateType;
import org.apache.qpid.proton.message.Message;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Vertx;

public class AmqpFailureHandlerTest extends AmqpTestBase {

    private WeldContainer container;
    private MockServer server;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }

        if (server != null) {
            server.close();
        }
    }

    private MyReceiverBean deploy() {
        Weld weld = new Weld();
        weld.addBeanClass(MyReceiverBean.class);

        container = weld.initialize();
        return container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
    }

    private MyReceiverBeanRecovering deployRecovering() {
        Weld weld = new Weld();
        weld.addBeanClass(MyReceiverBeanRecovering.class);

        container = weld.initialize();
        return container.getBeanManager().createInstance().select(MyReceiverBeanRecovering.class).get();
    }

    @Test
    @Timeout(30)
    public void testFailStrategy() throws Exception {
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        writeDefaultFailConfig(server.actualPort());
        MyReceiverBean bean = deploy();

        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();

        await().atMost(6, TimeUnit.SECONDS).until(() -> bean.list().size() >= 3);
        // Other messages should not have been received.
        assertThat(bean.list()).containsExactly(1, 2, 3);

        await().atMost(6, TimeUnit.SECONDS).until(() -> !isAmqpConnectorReady(connector));
        await().atMost(6, TimeUnit.SECONDS).until(() -> !isAmqpConnectorAlive(connector));

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= 2);

        for (int i = 0; i < dispositionsReceived.size(); i++) {
            DispositionRecord record = dispositionsReceived.get(i);

            int messageNum = i + 1;
            assertThat(messageNum).isLessThan(3);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();
        }
    }

    @Test
    @Timeout(30)
    public void testAcceptStrategy() throws Exception {
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        writeAcceptConfig(server.actualPort());
        MyReceiverBean bean = deploy();

        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();

        await().atMost(6, TimeUnit.SECONDS).until(() -> bean.list().size() >= 10);
        // All messages should have been received.
        assertThat(bean.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        assertThat(isAmqpConnectorAlive(connector)).isTrue();
        assertThat(isAmqpConnectorReady(connector)).isTrue();

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= 10);

        for (int i = 0; i < dispositionsReceived.size(); i++) {
            DispositionRecord record = dispositionsReceived.get(i);

            int messageNum = i + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();
        }
    }

    @Test
    @Timeout(30)
    public void testReleaseStrategy() throws Exception {
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        writeReleaseConfig(server.actualPort());

        MyReceiverBeanRecovering bean = deployRecovering();

        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();

        await().atMost(6, TimeUnit.SECONDS).until(() -> bean.list().size() >= 13);
        // All messages should have been received, with every 3rd released, which are then re-delivered and accepted.
        assertThat(bean.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 6, 9);

        assertThat(isAmqpConnectorAlive(connector)).isTrue();
        assertThat(isAmqpConnectorReady(connector)).isTrue();

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= 13);

        for (int i = 0; i < msgCount; i++) {
            DispositionRecord record = dispositionsReceived.get(i);

            int messageNum = i + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            if (messageNum % 3 == 0) {
                assertThat(record.getState()).isInstanceOf(Released.class);
            } else {
                assertThat(record.getState()).isInstanceOf(Accepted.class);
            }
            assertThat(record.isSettled()).isTrue();
        }

        for (int i = 0; i < msgCount / 3; i++) {
            DispositionRecord record = dispositionsReceived.get(msgCount + i);

            int messageNum = 3 * (i + 1);
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();
        }
    }

    @Test
    @Timeout(30)
    public void testRejectStrategy() throws Exception {
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        writeRejectConfig(server.actualPort());

        MyReceiverBean bean = deploy();

        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();

        await().atMost(6, TimeUnit.SECONDS).until(() -> bean.list().size() >= 10);
        // All messages should have been received, with every 3rd rejected, which are then NOT re-delivered.
        assertThat(bean.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        assertThat(isAmqpConnectorAlive(connector)).isTrue();
        assertThat(isAmqpConnectorReady(connector)).isTrue();

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= 10);

        for (int i = 0; i < msgCount; i++) {
            DispositionRecord record = dispositionsReceived.get(i);

            int messageNum = i + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            if (messageNum % 3 == 0) {
                assertThat(record.getState()).isInstanceOf(Rejected.class);
            } else {
                assertThat(record.getState()).isInstanceOf(Accepted.class);
            }
            assertThat(record.isSettled()).isTrue();
        }
    }

    @Test
    @Timeout(30)
    public void testModifiedFailedStrategy() throws Exception {
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        writeModifiedFailedConfig(server.actualPort());

        MyReceiverBeanRecovering bean = deployRecovering();

        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();

        await().atMost(6, TimeUnit.SECONDS).until(() -> bean.list().size() >= 13);
        // All messages should have been received, with every 3rd modified-failed, which are then re-delivered and accepted.
        assertThat(bean.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 6, 9);

        assertThat(isAmqpConnectorAlive(connector)).isTrue();
        assertThat(isAmqpConnectorReady(connector)).isTrue();

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= 13);

        for (int i = 0; i < msgCount; i++) {
            DispositionRecord record = dispositionsReceived.get(i);

            int messageNum = i + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            if (messageNum % 3 == 0) {
                assertThat(record.getState()).isInstanceOf(Modified.class);
                Modified state = (Modified) record.getState();

                assertThat(state.getDeliveryFailed()).isTrue();
                assertThat(state.getUndeliverableHere()).isFalse();
            } else {
                assertThat(record.getState()).isInstanceOf(Accepted.class);
            }
            assertThat(record.isSettled()).isTrue();
        }

        for (int i = 0; i < msgCount / 3; i++) {
            DispositionRecord record = dispositionsReceived.get(msgCount + i);

            int messageNum = 3 * (i + 1);
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();
        }
    }

    @Test
    @Timeout(30)
    public void testModifiedFailedUndeliverableHereStrategy() throws Exception {
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        writeModifiedFailedUndeliverableConfig(server.actualPort());

        MyReceiverBeanRecovering bean = deployRecovering();

        AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();

        await().atMost(6, TimeUnit.SECONDS).until(() -> bean.list().size() >= 10);
        // All messages should have been received, with every 3rd modified-failed, which are then NOT re-delivered.
        assertThat(bean.list()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        assertThat(isAmqpConnectorAlive(connector)).isTrue();
        assertThat(isAmqpConnectorReady(connector)).isTrue();

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= 10);

        for (int i = 0; i < msgCount; i++) {
            DispositionRecord record = dispositionsReceived.get(i);

            int messageNum = i + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            if (messageNum % 3 == 0) {
                assertThat(record.getState()).isInstanceOf(Modified.class);
                Modified state = (Modified) record.getState();

                assertThat(state.getDeliveryFailed()).isTrue();
                assertThat(state.getUndeliverableHere()).isTrue();
            } else {
                assertThat(record.getState()).isInstanceOf(Accepted.class);
            }
            assertThat(record.isSettled()).isTrue();
        }
    }

    private MapBasedConfig createBaseConnectorConfig(int port) {
        MapBasedConfig config = new MapBasedConfig();
        config.put("mp.messaging.incoming.amqp.connector", AmqpConnector.CONNECTOR_NAME);
        config.put("mp.messaging.incoming.amqp.host", "localhost");
        config.put("mp.messaging.incoming.amqp.port", port);
        config.put("mp.messaging.incoming.amqp.tracing-enabled", false);

        return config;
    }

    private void writeDefaultFailConfig(int port) {
        // fail is the default, so we dont specify a failure-strategy
        createBaseConnectorConfig(port)
                .put("mp.messaging.incoming.amqp.address", "default-fail")
                .write();
    }

    private void writeAcceptConfig(int port) {
        createBaseConnectorConfig(port)
                .put("mp.messaging.incoming.amqp.address", "accept")
                .put("mp.messaging.incoming.amqp.failure-strategy", "accept")
                .write();
    }

    private void writeRejectConfig(int port) {
        createBaseConnectorConfig(port)
                .put("mp.messaging.incoming.amqp.address", "reject")
                .put("mp.messaging.incoming.amqp.failure-strategy", "reject")
                .write();
    }

    private void writeReleaseConfig(int port) {
        createBaseConnectorConfig(port)
                .put("mp.messaging.incoming.amqp.address", "release")
                .put("mp.messaging.incoming.amqp.failure-strategy", "release")
                .write();
    }

    private void writeModifiedFailedConfig(int port) {
        createBaseConnectorConfig(port)
                .put("mp.messaging.incoming.amqp.address", "modified-failed")
                .put("mp.messaging.incoming.amqp.failure-strategy", "modified-failed")
                .write();
    }

    private void writeModifiedFailedUndeliverableConfig(int port) {
        createBaseConnectorConfig(port)
                .put("mp.messaging.incoming.amqp.address", "modified-failed-undeliverable-here")
                .put("mp.messaging.incoming.amqp.failure-strategy", "modified-failed-undeliverable-here")
                .write();
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        // Receiver that fails to process every 3rd message [payload] of the
        // original messages, every time that the payload is seen.
        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("amqp")
        public CompletionStage<Void> process(AmqpMessage<Integer> record) {
            Integer payload = record.getPayload();
            received.add(payload);
            if (payload != 0 && payload % 3 == 0) {
                return record.nack(new IllegalArgumentException("nack 3 - " + payload));
            }
            return record.ack();
        }

        public List<Integer> list() {
            return received;
        }
    }

    @ApplicationScoped
    public static class MyReceiverBeanRecovering {
        // Receiver that fails to process every 3rd message [payload] of the
        // original messages the first time it is seen, then succeeds if seen again.
        private final List<Integer> received = new CopyOnWriteArrayList<>();
        private final List<Integer> failed = new CopyOnWriteArrayList<>();

        @Incoming("amqp")
        public CompletionStage<Void> process(AmqpMessage<Integer> record) {
            Integer payload = record.getPayload();
            received.add(payload);
            if (payload != 0 && payload % 3 == 0 && !failed.contains(payload)) {
                failed.add(payload);
                return record.nack(new IllegalArgumentException("nack 3 - " + payload));
            }
            return record.ack();
        }

        public List<Integer> list() {
            return received;
        }
    }

    @SuppressWarnings("incomplete-switch")
    private MockServer setupMockServer(int msgCount, List<DispositionRecord> dispositions, Vertx vertx)
            throws Exception {
        AtomicInteger sent = new AtomicInteger(1);

        return new MockServer(vertx, serverConnection -> {
            serverConnection.openHandler(serverSender -> {
                serverConnection.closeHandler(x -> serverConnection.close());
                serverConnection.open();
            });

            serverConnection.sessionOpenHandler(serverSession -> {
                serverSession.closeHandler(x -> serverSession.close());
                serverSession.open();
            });

            serverConnection.senderOpenHandler(serverSender -> {
                serverSender.sendQueueDrainHandler(x -> {
                    while (sent.get() <= msgCount && !serverSender.sendQueueFull()) {
                        final Message m = Proton.message();
                        final int i = sent.getAndIncrement();
                        m.setBody(new AmqpValue(i));

                        serverSender.send(m, delivery -> {
                            DeliveryState deliveryState = delivery.getRemoteState();
                            dispositions.add(new DispositionRecord(i, deliveryState, delivery.remotelySettled()));

                            if (deliveryState != null) {
                                DeliveryStateType type = deliveryState.getType();
                                switch (type) {
                                    // This will only re-send a given message one time, that is sufficient for these tests.
                                    case Released:
                                        serverSender.send(m, delivery2 -> {
                                            dispositions.add(new DispositionRecord(i, delivery2.getRemoteState(),
                                                    delivery2.remotelySettled()));
                                        });
                                        break;
                                    case Modified:
                                        Modified ds = (Modified) deliveryState;
                                        if (!Boolean.TRUE.equals(ds.getUndeliverableHere())) {
                                            serverSender.send(m, delivery2 -> {
                                                dispositions.add(new DispositionRecord(i, delivery2.getRemoteState(),
                                                        delivery2.remotelySettled()));
                                            });
                                        }
                                        break;
                                }
                            }
                        });
                    }
                });

                serverSender.open();
            });
        });
    }
}
