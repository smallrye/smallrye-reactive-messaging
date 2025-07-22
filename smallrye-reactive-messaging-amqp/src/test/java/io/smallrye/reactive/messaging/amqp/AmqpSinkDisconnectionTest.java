package io.smallrye.reactive.messaging.amqp;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class AmqpSinkDisconnectionTest extends AmqpBrokerTestBase {

    @Test
    void testEagerOutgoingChannelDisconnection() {
        Weld weld = new Weld();
        weld.addBeanClass(MyProducer.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.address", "test")
                .with("mp.messaging.outgoing.data.host", host)
                .with("mp.messaging.outgoing.data.port", port)
                .with("mp.messaging.outgoing.data.username", username)
                .with("mp.messaging.outgoing.data.password", password)
                .with("mp.messaging.outgoing.data.lazy-client", false)
                .with("mp.messaging.outgoing.data.tracing-enabled", false)
                .write();

        try (WeldContainer container = weld.initialize()) {
            AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                    ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
            await().until(() -> isAmqpConnectorReady(connector) && isAmqpConnectorAlive(connector));

            MyProducer producer = container.getBeanManager().createInstance().select(MyProducer.class).get();
            producer.produce("a");
            producer.produce("b");
            producer.produce("c");

            stopBroker();
            await().pollDelay(2, SECONDS).until(() -> !isAmqpConnectorAlive(connector));
            startBroker();
            System.out.println("Broker restarted");
            await().until(() -> isAmqpConnectorAlive(connector));

            System.out.println("Resending messages...");
            producer.produce("d");
            producer.produce("e");
            producer.produce("f");
        }
    }

    @Test
    void testLazyOutgoingChannelDisconnection() {
        Weld weld = new Weld();
        weld.addBeanClass(MyProducer.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.address", "test")
                .with("mp.messaging.outgoing.data.host", host)
                .with("mp.messaging.outgoing.data.port", port)
                .with("mp.messaging.outgoing.data.username", username)
                .with("mp.messaging.outgoing.data.password", password)
                .with("mp.messaging.outgoing.data.tracing-enabled", false)
                .write();

        try (WeldContainer container = weld.initialize()) {
            AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                    ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
            await().until(() -> isAmqpConnectorReady(connector) && isAmqpConnectorAlive(connector));

            MyProducer producer = container.getBeanManager().createInstance().select(MyProducer.class).get();
            producer.produce("a");
            producer.produce("b");
            producer.produce("c");

            stopBroker();
            await().pollDelay(2, SECONDS).until(() -> !isAmqpConnectorAlive(connector));
            startBroker();
            System.out.println("Broker restarted");

            System.out.println("Resending messages...");
            producer.produce("d");
            producer.produce("e");
            producer.produce("f");
            await().until(() -> isAmqpConnectorAlive(connector));
        }
    }

    @ApplicationScoped
    public static class MyProducer {

        @Inject
        @Channel("data")
        MutinyEmitter<String> emitter;

        public void produce(String item) {
            emitter.sendAndAwait(item);
        }
    }
}
