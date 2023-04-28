package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import io.vertx.core.json.JsonObject;

public class MessageConverterTest extends MqttTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    @Test
    public void testJsonObjectConverter() {
        String topic = UUID.randomUUID().toString();
        Weld weld = baseWeld(getConfig(topic));
        weld.addBeanClass(JsonObjectConsumer.class);
        container = weld.initialize();

        JsonObjectConsumer bean = container.getBeanManager().createInstance().select(JsonObjectConsumer.class).get();
        MqttConnector mqttConnector = this.container.select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get();

        await().until(() -> mqttConnector.getReadiness().isOk());

        assertThat(bean.counts()).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produce(topic, 10, null,
                () -> JsonObject.of("count", counter.getAndIncrement()).toBuffer().getBytes())).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.counts().size() >= 10);
        assertThat(bean.counts())
                .extracting(j -> j.getInteger("count"))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testStringConverter() {
        String topic = UUID.randomUUID().toString();
        Weld weld = baseWeld(getConfig(topic));
        weld.addBeanClass(StringConsumer.class);
        container = weld.initialize();

        StringConsumer bean = container.getBeanManager().createInstance().select(StringConsumer.class).get();
        MqttConnector mqttConnector = this.container.select(MqttConnector.class, ConnectorLiteral.of("smallrye-mqtt")).get();

        await().until(() -> mqttConnector.getReadiness().isOk());

        assertThat(bean.counts()).isEmpty();

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceStrings(topic, 10, null,
                () -> String.valueOf(counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.counts().size() >= 10);
        assertThat(bean.counts())
                .extracting(s -> Integer.valueOf(s))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private MapBasedConfig getConfig(String topic) {
        String count = "mp.messaging.incoming.count.";
        Map<String, Object> config = new HashMap<>();

        config.put(count + "topic", topic);
        config.put(count + "connector", MqttConnector.CONNECTOR_NAME);
        config.put(count + "host", System.getProperty("mqtt-host"));
        config.put(count + "port", Integer.valueOf(System.getProperty("mqtt-port")));
        config.put(count + "qos", 1);
        if (System.getProperty("mqtt-user") != null) {
            config.put(count + "username", System.getProperty("mqtt-user"));
            config.put(count + "password", System.getProperty("mqtt-pwd"));
        }
        return new MapBasedConfig(config);
    }

    @ApplicationScoped
    public static class JsonObjectConsumer {

        List<JsonObject> counts = new CopyOnWriteArrayList<>();

        @Incoming("count")
        public void processPrices(JsonObject count) {
            counts.add(count);
        }

        public List<JsonObject> counts() {
            return counts;
        }
    }

    @ApplicationScoped
    public static class StringConsumer {

        List<String> counts = new CopyOnWriteArrayList<>();

        @Incoming("count")
        public void processPrices(String count) {
            counts.add(count);
        }

        public List<String> counts() {
            return counts;
        }
    }

}
