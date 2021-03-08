package io.smallrye.reactive.messaging.mqtt.hivemq;

import io.smallrye.reactive.messaging.mqtt.MqttFactory;

public class HiveMQMqttSourceTest extends io.smallrye.reactive.messaging.mqtt.MqttSourceTest {

    MqttFactory hiveMQMqttFactory = new HiveMQMqttFactory();

    protected MqttFactory mqttFactory() {
        return hiveMQMqttFactory;
    }

}
