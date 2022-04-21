package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Vertx;

public class AmqpWithConverterTest extends AmqpTestBase {

    private SeContainer container;
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

    @Test
    @Timeout(30)
    public void testAckWithConverter() throws Exception {
        String address = UUID.randomUUID().toString();
        int msgCount = 10;
        List<DispositionRecord> dispositionsReceived = Collections.synchronizedList(new ArrayList<>(msgCount));

        server = setupMockServer(msgCount, dispositionsReceived, executionHolder.vertx().getDelegate());

        SeContainerInitializer weld = Weld.newInstance();
        new MapBasedConfig()
                .with("mp.messaging.incoming.in.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.host", "localhost")
                .with("mp.messaging.incoming.in.port", server.actualPort())
                .with("mp.messaging.incoming.in.address", address)
                .with("mp.messaging.incoming.in.tracing-enabled", false)
                .write();
        weld.addBeanClasses(DummyConverter.class, MyApp.class);
        container = weld.initialize();

        MyApp app = container.getBeanManager().createInstance().select(MyApp.class).get();
        List<DummyData> appList = app.list();
        await().atMost(5, TimeUnit.SECONDS).until(() -> appList.size() >= msgCount);

        AtomicInteger count = new AtomicInteger();
        appList.forEach(dummy -> {
            assertThat(dummy).isInstanceOf(DummyData.class);
            count.incrementAndGet();
        });
        assertThat(count.get()).isEqualTo(msgCount);

        await().atMost(2, TimeUnit.SECONDS).until(() -> dispositionsReceived.size() >= msgCount);

        count.set(0);
        dispositionsReceived.forEach(record -> {
            int messageNum = count.get() + 1;
            assertThat(messageNum).isLessThanOrEqualTo(msgCount);

            assertThat(record.getMessageNumber()).isEqualTo(messageNum);
            assertThat(record.getState()).isInstanceOf(Accepted.class);
            assertThat(record.isSettled()).isTrue();

            count.incrementAndGet();
        });

        assertThat(count.get()).isEqualTo(msgCount);
    }

    @ApplicationScoped
    public static class DummyConverter implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return true;
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return in.withPayload(new DummyData());
        }
    }

    @ApplicationScoped
    public static class MyApp {
        List<DummyData> list = new ArrayList<>();

        @Incoming("in")
        public void consume(DummyData data) {
            list.add(data);
        }

        public List<DummyData> list() {
            return list;
        }
    }

    public static class DummyData {

    }

    private MockServer setupMockServer(int msgCount, List<DispositionRecord> dispositions, Vertx vertx) throws Exception {
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
                        final org.apache.qpid.proton.message.Message m = Proton.message();
                        final int i = sent.getAndIncrement();
                        m.setBody(new AmqpValue(i));

                        serverSender.send(m, delivery -> {
                            DeliveryState deliveryState = delivery.getRemoteState();
                            dispositions.add(new DispositionRecord(i, deliveryState, delivery.remotelySettled()));
                        });
                    }
                });

                serverSender.open();
            });
        });
    }
}
