package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class MultipleTopicsConsumptionTest extends MqttTestBase {

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
        Weld weld = baseWeld(getConfig("my-id"));
        weld.addBeanClass(Consumers.class);
        container = weld.initialize();

        Consumers bean = container.getBeanManager().createInstance().select(Consumers.class).get();

        await().until(() -> this.container.select(MediatorManager.class).get().isInitialized());

        await()
                .until(() -> this.container.select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get().isReady());

        assertThat(bean.prices()).isEmpty();
        assertThat(bean.products()).isEmpty();

        AtomicInteger counter1 = new AtomicInteger();
        AtomicInteger counter2 = new AtomicInteger();
        new Thread(() -> usage.produceIntegers("prices", 10, null, counter1::getAndIncrement))
                .start();
        new Thread(() -> usage.produceIntegers("products", 10, null, counter2::getAndIncrement))
                .start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.products().size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.prices().size() >= 10);
    }

    @Test
    public void testWithoutClientId() {
        Clients.clear();
        Weld weld = baseWeld(getConfig(null));
        weld.addBeanClass(Consumers.class);
        container = weld.initialize();

        Consumers bean = container.getBeanManager().createInstance().select(Consumers.class).get();

        await().until(() -> this.container.select(MediatorManager.class).get().isInitialized());

        await()
                .until(() -> this.container.select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get().isReady());

        assertThat(bean.prices()).isEmpty();
        assertThat(bean.products()).isEmpty();

        AtomicInteger counter1 = new AtomicInteger();
        AtomicInteger counter2 = new AtomicInteger();
        new Thread(() -> usage.produceIntegers("prices", 10, null, counter1::getAndIncrement))
                .start();
        new Thread(() -> usage.produceIntegers("products", 10, null, counter2::getAndIncrement))
                .start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.products().size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.prices().size() >= 10);
    }

    private MapBasedConfig getConfig(String id) {
        String prices = "mp.messaging.incoming.prices.";
        String products = "mp.messaging.incoming.products.";
        Map<String, Object> config = new HashMap<>();

        config.put(prices + "topic", "prices");
        config.put(prices + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(prices + "host", System.getProperty("mqtt-host"));
        config.put(prices + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        config.put(prices + "qos", 1);
        if (id != null) {
            config.put(prices + "client-id", id);
        }
        if (System.getProperty("mqtt-user") != null) {
            config.put(prices + "username", System.getProperty("mqtt-user"));
            config.put(prices + "password", System.getProperty("mqtt-pwd"));
        }

        config.put(products + "topic", "products");
        config.put(products + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(products + "host", System.getProperty("mqtt-host"));
        config.put(products + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        config.put(products + "qos", 1);
        if (id != null) {
            config.put(products + "client-id", id);
        }
        if (System.getProperty("mqtt-user") != null) {
            config.put(products + "username", System.getProperty("mqtt-user"));
            config.put(products + "password", System.getProperty("mqtt-pwd"));
        }

        return new MapBasedConfig(config);
    }

    @ApplicationScoped
    public static class Consumers {

        List<String> prices = new CopyOnWriteArrayList<>();
        List<String> products = new CopyOnWriteArrayList<>();

        @Incoming("prices")
        public void processPrices(byte[] priceRaw) {
            prices.add(new String(priceRaw));
        }

        @Incoming("products")
        public void processProducts(byte[] productRaw) {
            products.add(new String(productRaw));
        }

        public List<String> prices() {
            return prices;
        }

        public List<String> products() {
            return products;
        }
    }

}
