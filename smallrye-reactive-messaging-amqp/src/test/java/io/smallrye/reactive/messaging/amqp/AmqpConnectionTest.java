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

public class AmqpConnectionTest extends AmqpTestBase {

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
    public void testVirtualhostOptionSetsOpenHostname() throws Exception {
        //NOTE: this only tests the Open hostname aspect, not the further TLS SNI aspect the option also sets.
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<ProtonConnection> connectionRef = new AtomicReference<>();

        server = setupMockServerForVirtualhostOption(connectionRef, dispositionsReceived,
                executionHolder.vertx().getDelegate());

        Weld weld = new Weld();
        weld.addBeanClass(MyConsumer.class);

        String serverDnsHost = "localhost";
        String virtualHostname = "some-other-virtual-hostname";

        assertThat(serverDnsHost).isNotEqualTo(virtualHostname);

        MapBasedConfig config = new MapBasedConfig()
                .put("mp.messaging.incoming.messages-in.connector", AmqpConnector.CONNECTOR_NAME)
                .put("mp.messaging.incoming.messages-in.host", serverDnsHost)
                .put("mp.messaging.incoming.messages-in.port", server.actualPort())
                .put("mp.messaging.incoming.messages-in.virtual-host", virtualHostname)
                .put("mp.messaging.incoming.messages-in.tracing-enabled", false);
        config.write();

        container = weld.initialize();

        MyConsumer consumer = container.getBeanManager().createInstance().select(MyConsumer.class).get();

        await().atMost(6, TimeUnit.SECONDS).until(() -> consumer.list().size() >= 1);

        assertThat(consumer.list()).containsExactly("Virtual");

        await().atMost(3, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= 1);

        AtomicInteger count = new AtomicInteger();
        dispositionsReceived.forEach(record -> {
            assertThat(record.getMessageNumber()).isEqualTo(count.get() + 1);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });
        assertThat(count.get()).isEqualTo(1);

        // Verify details of the connection created
        ProtonConnection serverConnection = connectionRef.get();
        assertThat(serverConnection).isNotNull();

        assertThat(serverConnection.getRemoteHostname()).isEqualTo(virtualHostname);
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

    private MockServer setupMockServerForVirtualhostOption(AtomicReference<ProtonConnection> connectionRef,
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
                serverSender.open();

                // Just immediately buffer a message.
                sendMessage(serverSender, "Virtual", 1, dispositions);
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
