package io.smallrye.reactive.messaging.mqtt.hivemq;

import org.jboss.weld.environment.se.WeldContainer;

import io.smallrye.reactive.messaging.mqtt.DynamicMqttTopicSourceTest;
import io.smallrye.reactive.messaging.mqtt.MqttFactory;

public class HiveMQDynamicMqttTopicSourceTest extends DynamicMqttTopicSourceTest {

    private WeldContainer container;

    private MqttFactory hivemqMqttFactory = new HiveMQMqttFactory();

    protected MqttFactory mqttFactory() {
        return hivemqMqttFactory;
    }

}
