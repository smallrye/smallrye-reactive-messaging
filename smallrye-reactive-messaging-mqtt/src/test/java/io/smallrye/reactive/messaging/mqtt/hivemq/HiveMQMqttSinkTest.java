package io.smallrye.reactive.messaging.mqtt.hivemq;

import io.smallrye.reactive.messaging.mqtt.MqttFactory;

public class HiveMQMqttSinkTest extends io.smallrye.reactive.messaging.mqtt.MqttSinkTest {

    private MqttFactory hivemqMqttFactory = new HiveMQMqttFactory();

    protected MqttFactory mqttFactory() {
        return hivemqMqttFactory;
    }

}
