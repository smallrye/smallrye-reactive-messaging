package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.MqttConnector.CONNECTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class HealthCheckTest extends MqttTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
    }

    private void awaitAndVerify() {

        container.getBeanManager().createInstance().select(JustToStartConnector.class).get();
        MqttConnector connector = this.container.select(MqttConnector.class, ConnectorLiteral.of(CONNECTOR_NAME)).get();

        await()
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> connector.getReadiness().isOk());

        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();

        assertThat(liveness.getChannels().size()).isEqualTo(2);

        mosquitto.stop();

        await()
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> !connector.getReadiness().isOk());

        liveness = getHealth().getLiveness();
        assertThat(liveness.isOk()).isTrue();

    }

    @Test
    public void testWithDash() {
        Weld weld = baseWeld(getConfig());
        weld.addBeanClass(JustToStartConnector.class);

        container = weld.initialize();

        awaitAndVerify();
    }

    private MapBasedConfig getConfig() {
        String prefix = "mp.messaging.outgoing.out.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", CONNECTOR_NAME);
        config.put(prefix + "host", System.getProperty("mqtt-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        if (System.getProperty("mqtt-user") != null) {
            config.put(prefix + "username", System.getProperty("mqtt-user"));
            config.put(prefix + "password", System.getProperty("mqtt-pwd"));
        }

        prefix = "mp.messaging.incoming.in.";
        config.put(prefix + "connector", CONNECTOR_NAME);
        config.put(prefix + "host", System.getProperty("mqtt-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        if (System.getProperty("mqtt-user") != null) {
            config.put(prefix + "username", System.getProperty("mqtt-user"));
            config.put(prefix + "password", System.getProperty("mqtt-pwd"));
        }
        return new MapBasedConfig(config);
    }

    public HealthCenter getHealth() {
        if (container == null) {
            throw new IllegalStateException("Application not started");
        }
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }

    public boolean isStarted() {
        return getHealth().getStartup().isOk();
    }

    public boolean isReady() {
        return getHealth().getReadiness().isOk();
    }

    public boolean isAlive() {
        return getHealth().getLiveness().isOk();
    }

    @ApplicationScoped
    public static class JustToStartConnector {

        @Inject
        @Channel("out")
        Emitter<String> emitter;

        @Incoming("in")
        public CompletionStage<Void> received(MqttMessage<byte[]> message) {
            return message.ack();
        }

    }

}
