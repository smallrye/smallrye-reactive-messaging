package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MqttCustomOptionTest extends MqttTestBase {

    @Test
    public void testWithChannelPort() {
        Weld weld = new Weld();
        weld.addBeanClass(MyOptionCustomizer.class).addBeanClass(Application.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.host", "localhost")
                .with("mp.messaging.outgoing.sink.connector", MqttConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.client-options-name", "options")
                .with("mp.messaging.outgoing.sink.port", 1234) // Wrong on purpose
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)

                .with("mp.messaging.incoming.source.host", "localhost")
                .with("mp.messaging.incoming.source.connector", MqttConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.client-options-name", "options")
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .write();

        try (WeldContainer container = weld.initialize()) {
            MqttConnector connector = container.getBeanManager().createInstance().select(MqttConnector.class,
                    ConnectorLiteral.of(MqttConnector.CONNECTOR_NAME)).get();

            await().pollDelay(Duration.ofSeconds(1)).until(() -> !connector.getReadiness().isOk());
        }
    }

    @Test
    public void testMissingOptions() {
        Weld weld = new Weld();
        weld.addBeanClass(MyOptionCustomizer.class).addBeanClass(Application.class);

        new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.host", "localhost")
                .with("mp.messaging.outgoing.sink.connector", MqttConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.client-options-name", "missing")
                .with("mp.messaging.outgoing.sink.port", 1234) // Wrong on purpose
                .with("mp.messaging.outgoing.sink.tracing-enabled", false)

                .with("mp.messaging.incoming.source.host", "localhost")
                .with("mp.messaging.incoming.source.connector", MqttConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.client-options-name", "options")
                .with("mp.messaging.incoming.source.tracing-enabled", false)
                .write();

        assertThatThrownBy(weld::initialize)
                .isInstanceOf(DeploymentException.class);
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
    public static class MyOptionCustomizer {

        @Produces
        @Identifier("options")
        public MqttClientSessionOptions options() {
            return new MqttClientSessionOptions()
                    .setHostname(System.getProperty("mqtt-host"))
                    .setPort(9999)
                    .setUsername(System.getProperty("mqtt-user"))
                    .setPassword(System.getProperty("mqtt-pwd"));
        }
    }

}
