package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ConnectionSharingTest extends MqttTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    @Test
    public void testWithClientId() {
        Clients.clear();
        Weld weld = baseWeld(getConfig());
        weld.addBeanClass(App.class);
        container = weld.initialize();

        App bean = container.getBeanManager().createInstance().select(App.class).get();

        await().until(() -> this.container.select(MediatorManager.class).get().isInitialized());

        await()
                .until(() -> this.container.select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get().isReady());

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.prices().size() >= 10);
        assertThat(bean.prices()).isNotEmpty();
    }

    private MapBasedConfig getConfig() {
        String topic = UUID.randomUUID().toString();
        String prices = "mp.messaging.incoming.prices.";
        String generator = "mp.messaging.outgoing.to-mqtt.";
        Map<String, Object> config = new HashMap<>();

        config.put(prices + "topic", topic);
        config.put(prices + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(prices + "host", System.getProperty("mqtt-host"));
        config.put(prices + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        config.put(prices + "qos", 1);
        config.put(prices + "client-id", "my-id");
        if (System.getProperty("mqtt-user") != null) {
            config.put(prices + "username", System.getProperty("mqtt-user"));
            config.put(prices + "password", System.getProperty("mqtt-pwd"));
        }

        config.put(generator + "topic", topic);
        config.put(generator + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(generator + "host", System.getProperty("mqtt-host"));
        config.put(generator + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        config.put(generator + "qos", 1);
        config.put(generator + "client-id", "my-id");
        if (System.getProperty("mqtt-user") != null) {
            config.put(generator + "username", System.getProperty("mqtt-user"));
            config.put(generator + "password", System.getProperty("mqtt-pwd"));
        }

        return new MapBasedConfig(config);
    }

    @ApplicationScoped
    public static class App {

        List<String> prices = new CopyOnWriteArrayList<>();
        Random random = new Random();

        @Incoming("prices")
        public void processPrices(byte[] priceRaw) {
            prices.add(new String(priceRaw));
        }

        @Outgoing("to-mqtt")
        public Multi<Integer> generate() {
            return Multi.createFrom().ticks().every(Duration.ofMillis(100))
                    .map(l -> random.nextInt(100))
                    .onOverflow().drop()
                    .transform().byTakingFirstItems(100);
        }

        public List<String> prices() {
            return prices;
        }

    }

}
