package io.smallrye.reactive.messaging.mqtt.hivemq;

import io.smallrye.reactive.messaging.mqtt.FailureHandlerTest;
import io.smallrye.reactive.messaging.mqtt.MqttFactory;

public class HiveMQFailureHandlerTest extends FailureHandlerTest {
    private MqttFactory hivemqMqttFactory = new HiveMQMqttFactory();

    protected MqttFactory mqttFactory() {
        return hivemqMqttFactory;
    }

}
