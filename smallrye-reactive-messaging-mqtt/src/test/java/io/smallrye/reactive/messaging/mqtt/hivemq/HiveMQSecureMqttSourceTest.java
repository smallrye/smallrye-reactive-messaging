package io.smallrye.reactive.messaging.mqtt.hivemq;

import io.smallrye.reactive.messaging.mqtt.MqttFactory;
import io.smallrye.reactive.messaging.mqtt.SecureMqttSourceTest;

public class HiveMQSecureMqttSourceTest extends SecureMqttSourceTest {

    MqttFactory hiveMQMqttFactory = new HiveMQMqttFactory();

    protected MqttFactory mqttFactory() {
        return hiveMQMqttFactory;
    }

}
