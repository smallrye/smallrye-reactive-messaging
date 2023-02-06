package io.smallrye.reactive.messaging.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.amqp.AmqpClientOptions;

public class AmqpCustomerTest extends AmqpBrokerTestBase {

    @Test
    public void testWithCustomPort() {
        stopBroker();
        broker.start(9999);

        Weld weld = new Weld();
        weld.addBeanClass(MyAMQPOptionCustomizer.class).addBeanClass(Application.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.address", "test")
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.client-options-name", "options")
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)

                .with("mp.messaging.incoming.source.address", "test")
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.client-options-name", "options")
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .write();

        try (WeldContainer container = weld.initialize()) {
            AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                    ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();
            await().until(() -> isAmqpConnectorReady(connector));
            await().until(() -> isAmqpConnectorAlive(connector));

            Application application = container.getBeanManager().createInstance().select(Application.class).get();
            application.send("a");
            application.send("b");
            application.send("c");

            await().until(() -> application.list().size() == 3);
            assertThat(application.list()).containsExactly("a", "b", "c");
        }
    }

    @Test
    public void testWithChannelPort() {
        Weld weld = new Weld();
        weld.addBeanClass(MyAMQPOptionCustomizer.class).addBeanClass(Application.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.address", "test")
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.client-options-name", "options")
                .with("mp.messaging.outgoing.sink.port", 1234) // Wrong on purpose
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)

                .with("mp.messaging.incoming.source.address", "test")
                .with("mp.messaging.incoming.source.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.client-options-name", "options")
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .write();

        try (WeldContainer container = weld.initialize()) {
            AmqpConnector connector = container.getBeanManager().createInstance().select(AmqpConnector.class,
                    ConnectorLiteral.of(AmqpConnector.CONNECTOR_NAME)).get();

            await().pollDelay(Duration.ofSeconds(1)).until(() -> !isAmqpConnectorAlive(connector));
        }
    }

    @ApplicationScoped
    public static class Application {

        List<String> messages = new CopyOnWriteArrayList<>();

        @Incoming("source")
        void source(String m) {
            messages.add(m);
        }

        @Inject
        @Channel("sink")
        Emitter<String> emitter;

        void send(String s) {
            emitter.send(s);
        }

        public List<String> list() {
            return messages;
        }

    }

    @ApplicationScoped
    public static class MyAMQPOptionCustomizer {

        @Produces
        @Identifier("options")
        public AmqpClientOptions options() {
            return new AmqpClientOptions()
                    .setHost(System.getProperty("amqp-host"))
                    .setPort(9999)
                    .setUsername(System.getProperty("amqp-user"))
                    .setPassword(System.getProperty("amqp-pwd"));
        }
    }

}
