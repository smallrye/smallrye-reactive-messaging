package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Test a Multi-level wildcard. https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718107
 */
public class WildcardSubscriptionTest extends MqttTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    /**
     * If a Client subscribes to “sport/tennis/player1/#”, it would receive messages published on “sport/tennis/player1”
     */
    @Test
    public void testWildcardOnRootLevel() {
        Clients.clear();
        Weld weld = baseWeld(getConfig());
        weld.addBeanClass(Consumers.class);
        container = weld.initialize();

        Consumers bean = container.getBeanManager().createInstance().select(Consumers.class).get();
        MqttConnector mqttConnector = this.container.select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get();

        await().until(() -> mqttConnector.getReadiness().isOk());

        assertThat(bean.data()).isEmpty();

        AtomicInteger counter1 = new AtomicInteger();
        new Thread(() -> usage.produceIntegers("test-$topic-^mqtt", 10, null, counter1::getAndIncrement))
                .start();

        await().atMost(10, TimeUnit.SECONDS).until(() -> bean.data().size() >= 10);
    }

    /**
     * If a Client subscribes to “sport/tennis/player1/#”, it would receive messages published on “sport/tennis/player1/l1/l2”
     */
    @Test
    public void testWildcardOnChild() {
        Clients.clear();
        Weld weld = baseWeld(getConfig());
        weld.addBeanClass(Consumers.class);
        container = weld.initialize();

        Consumers bean = container.getBeanManager().createInstance().select(Consumers.class).get();
        MqttConnector mqttConnector = this.container.select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get();

        await().until(() -> mqttConnector.getReadiness().isOk());

        assertThat(bean.data()).isEmpty();

        AtomicInteger counter1 = new AtomicInteger();
        new Thread(() -> usage.produceIntegers("test-$topic-^mqtt/l1/l2", 10, null, counter1::getAndIncrement))
                .start();

        await().atMost(10, TimeUnit.SECONDS).until(() -> bean.data().size() >= 10);
    }

    @ApplicationScoped
    public static class Consumers {

        List<String> data = new CopyOnWriteArrayList<>();

        @Incoming("test-topic-mqtt")
        public void processPrices(byte[] priceRaw) {
            data.add(new String(priceRaw));
        }

        public List<String> data() {
            return data;
        }
    }

    private MapBasedConfig getConfig() {
        String prefix = "mp.messaging.incoming.test-topic-mqtt.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "topic", "test-$topic-^mqtt/#");
        config.put(prefix + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(prefix + "host", System.getProperty("mqtt-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        if (System.getProperty("mqtt-user") != null) {
            config.put(prefix + "username", System.getProperty("mqtt-user"));
            config.put(prefix + "password", System.getProperty("mqtt-pwd"));
        }
        return new MapBasedConfig(config);
    }

}
