package io.smallrye.reactive.messaging.mqtt;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Reproduce <a href="https://github.com/quarkusio/quarkus/issues/32462">#32462</a>.
 */
@Disabled
public class MultipleConnectorTest extends MqttTestBase {

    private WeldContainer container;

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        Clients.clear();
    }

    @Test
    void test() {
        Weld weld = baseWeld(multiConnectorConfig());
        weld.addBeanClass(MyConsumers.class);
        container = weld.initialize();

        usage.produceStrings("arconsis/test/quarkus", 1, null, () -> {
            return "hello";
        });

        MyConsumers consumers = container.select(MyConsumers.class).get();
        await()
                .pollDelay(Duration.ofSeconds(1))
                .until(() -> consumers.getListFromConsumer1().size() == 1);
        // Make sure we didn't get duplicates
        await()
                .pollDelay(Duration.ofSeconds(2))
                .until(() -> consumers.getListFromConsumer1().size() == 1);
    }

    private MapBasedConfig multiConnectorConfig() {
        var config = new MapBasedConfig();
        //# Consumer1
        String prefix = "mp.messaging.incoming.consumer1.";
        config.with(prefix, "connector", MqttConnector.CONNECTOR_NAME)
                .with(prefix, "host", System.getProperty("mqtt-host"))
                .with(prefix, "port", Integer.valueOf(System.getProperty("mqtt-port")))
                .with(prefix, "topic", "arconsis/#");

        if (System.getProperty("mqtt-user") != null) {
            config.with(prefix, "username", System.getProperty("mqtt-user"));
            config.with(prefix, "password", System.getProperty("mqtt-pwd"));
        }

        //# Consumer2
        prefix = "mp.messaging.incoming.consumer2";
        config.with(prefix, "connector", MqttConnector.CONNECTOR_NAME)
                .with(prefix, "host", System.getProperty("mqtt-host"))
                .with(prefix, "port", Integer.valueOf(System.getProperty("mqtt-port")))
                .with(prefix, "topic", "arconsis/test/#");

        if (System.getProperty("mqtt-user") != null) {
            config.with(prefix, "username", System.getProperty("mqtt-user"));
            config.with(prefix, "password", System.getProperty("mqtt-pwd"));
        }

        //# Consumer3
        prefix = "mp.messaging.incoming.consumer3";
        config.with(prefix, "connector", MqttConnector.CONNECTOR_NAME)
                .with(prefix, "host", System.getProperty("mqtt-host"))
                .with(prefix, "port", Integer.valueOf(System.getProperty("mqtt-port")))
                .with(prefix, "topic", "arconsis/test/quarkus");

        if (System.getProperty("mqtt-user") != null) {
            config.with(prefix, "username", System.getProperty("mqtt-user"));
            config.with(prefix, "password", System.getProperty("mqtt-pwd"));
        }

        return config;
    }

    @ApplicationScoped
    public static class MyConsumers {

        private final List<String> listFromConsumer1 = new CopyOnWriteArrayList<>();

        @Incoming("consumer1")
        public void consume1(String s) {
            listFromConsumer1.add(s);
        }

        public List<String> getListFromConsumer1() {
            return listFromConsumer1;
        }

    }

}
