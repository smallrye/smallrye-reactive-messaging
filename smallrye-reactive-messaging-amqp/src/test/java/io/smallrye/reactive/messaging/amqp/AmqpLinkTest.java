package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;

public class AmqpLinkTest extends AmqpTestBase {

    private WeldContainer container;
    private MockServer server;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }

        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testIncoming() throws Exception {
        doIncomingTestImpl(false);
    }

    @Test
    public void testIncomingDurable() throws Exception {
        doIncomingTestImpl(true);
    }

    private void doIncomingTestImpl(boolean durable) throws Exception {
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>());

        AtomicReference<ProtonConnection> connectionRef = new AtomicReference<>();
        AtomicReference<ProtonSender> senderRef = new AtomicReference<>();
        server = setupMockServer(connectionRef, senderRef, dispositionsReceived, executionHolder.vertx().getDelegate());

        Weld weld = new Weld();

        weld.addBeanClass(MyConsumer.class);

        String containerId = "myContainerId";
        String subscriptionName = "mySubName";
        String address = "people";

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.people-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.people-in.container-id", containerId)
                .put("mp.messaging.incoming.people-in.address", address)
                .put("mp.messaging.incoming.people-in.link-name", subscriptionName)
                .put("mp.messaging.incoming.people-in.host", "localhost")
                .put("mp.messaging.incoming.people-in.port", server.actualPort())
                .put("mp.messaging.incoming.people-in.tracing-enabled", false);
        if (durable) {
            config.put("mp.messaging.incoming.people-in.durable", true);
        }
        config.write();

        container = weld.initialize();

        MyConsumer consumer = container.getBeanManager().createInstance().select(MyConsumer.class).get();

        await().atMost(6, TimeUnit.SECONDS).until(() -> consumer.list().size() >= 3);

        assertThat(consumer.list()).containsExactly("Luke", "Leia", "Han");

        await().atMost(3, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= 3);

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            assertThat(record.getMessageNumber()).isEqualTo(count.get() + 1);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });
        assertThat(count.get()).isEqualTo(3);

        // Verify details of the connection and link created
        ProtonConnection serverConnection = connectionRef.get();
        assertThat(serverConnection).isNotNull();
        assertThat(serverConnection.getRemoteContainer()).isEqualTo(containerId);

        ProtonSender serverSender = senderRef.get();
        assertThat(serverSender).isNotNull();
        assertThat(serverSender.getName()).isEqualTo(subscriptionName);
        Source source = (org.apache.qpid.proton.amqp.messaging.Source) serverSender.getRemoteSource();
        assertThat(source).isNotNull();
        assertThat(source.getAddress()).isEqualTo(address);
        if (durable) {
            assertThat(source.getDurable()).isEqualTo(TerminusDurability.UNSETTLED_STATE);
            assertThat(source.getExpiryPolicy()).isEqualTo(TerminusExpiryPolicy.NEVER);
        } else {
            assertThat(source.getDurable()).isEqualTo(TerminusDurability.NONE);
            assertThat(source.getExpiryPolicy()).isNotEqualTo(TerminusExpiryPolicy.NEVER);
        }
    }

    @ApplicationScoped
    public static class MyConsumer {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("people-in")
        public void getPeople(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    private MockServer setupMockServer(AtomicReference<ProtonConnection> connectionRef, AtomicReference<ProtonSender> senderRef,
            List<DispositionRecord> dispositions, Vertx vertx) throws Exception {
        return new MockServer(vertx, serverConnection -> {
            connectionRef.compareAndSet(null, serverConnection);

            serverConnection.openHandler(serverSender -> {
                serverConnection.closeHandler(x -> serverConnection.close());
                serverConnection.open();
            });

            serverConnection.sessionOpenHandler(serverSession -> {
                serverSession.closeHandler(x -> serverSession.close());
                serverSession.open();
            });

            serverConnection.senderOpenHandler(serverSender -> {
                senderRef.compareAndSet(null, serverSender);
                serverSender.open();

                // Just immediately buffer some sends.
                sendMessage(serverSender, "Luke", 1, dispositions);
                sendMessage(serverSender, "Leia", 2, dispositions);
                sendMessage(serverSender, "Han", 3, dispositions);
            });
        });
    }

    private void sendMessage(ProtonSender serverSender, String content, int messageNumber,
            List<DispositionRecord> dispositions) {
        final org.apache.qpid.proton.message.Message msg = Proton.message();
        msg.setBody(new AmqpValue(content));

        serverSender.send(msg, delivery -> {
            DeliveryState deliveryState = delivery.getRemoteState();
            dispositions.add(new DispositionRecord(messageNumber, deliveryState, delivery.remotelySettled()));
        });
    }

}
