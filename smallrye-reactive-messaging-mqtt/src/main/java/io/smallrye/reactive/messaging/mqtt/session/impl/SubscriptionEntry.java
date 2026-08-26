package io.smallrye.reactive.messaging.mqtt.session.impl;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.smallrye.reactive.messaging.mqtt.session.RequestedQoS;

final class SubscriptionEntry {

    final RequestedQoS qos;
    final MqttSubscriptionOption option;

    SubscriptionEntry(RequestedQoS qos, MqttSubscriptionOption option) {
        this.qos = qos;
        this.option = option != null ? option
                : MqttSubscriptionOption.onlyFromQos(MqttQoS.valueOf(qos.toInteger()));
    }

}