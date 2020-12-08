package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class DynamicMqttSinkTest extends MqttTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
    }

    @Test
    public void testABeanProducingMessagesSentToMQTT() {
        Weld weld = baseWeld(getConfig());
        weld.addBeanClass(DynamicTopicProducingBean.class);

        final List<MqttMessage> rawMessages = new ArrayList<>(10);
        final List<String> topics = new CopyOnWriteArrayList<>();
        usage.consumeRaw("#", 10, 60, TimeUnit.SECONDS, null,
                (topic, msg) -> {
                    topics.add(topic);
                    rawMessages.add(msg);
                });

        container = weld.initialize();

        await().atMost(1, TimeUnit.MINUTES).until(() -> topics.size() >= 10);
        assertThat(topics.size()).isEqualTo(10);
        assertThat(rawMessages.size()).isEqualTo(10);
        MqttMessage firstMessage = rawMessages.get(0);
        assertThat(firstMessage.getQos()).isEqualTo(1);
        assertThat(firstMessage.isRetained()).isFalse();
    }

    private MapBasedConfig getConfig() {
        String prefix = "mp.messaging.outgoing.sink.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "topic", "sink");
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
