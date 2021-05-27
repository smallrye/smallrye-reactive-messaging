package io.smallrye.reactive.messaging.mqtt.session;

import io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions;

/**
 * The requested QoS level.
 * <p>
 * NOTE: This is missing QoS 2, as this mode is not properly supported by the session.
 */
public enum RequestedQoS {
    QOS_0(0),
    QOS_1(1);

    private final int value;

    RequestedQoS(int value) {
        this.value = value;
    }

    public int toInteger() {
        return this.value;
    }

    public static RequestedQoS valueOf(Integer qos) {
        if (qos == null) {
            return null;
        }
        switch (qos) {
            case 0:
                return RequestedQoS.QOS_0;
            case 1:
                return RequestedQoS.QOS_1;
            default:
                throw MqttExceptions.ex.illegalArgumentInvalidQoS(qos);
        }
    }
}
