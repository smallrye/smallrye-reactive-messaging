package io.smallrye.reactive.messaging.mqtt.session;

public interface SubscriptionEvent {
    Integer getQos();

    SubscriptionState getSubscriptionState();

    String getTopic();

    /**
     * @return the SUBACK reason code (MQTT 5.0). For v5, values {@literal >=} 0x80 indicate errors.
     *         Returns the granted QoS for v3.1.1.
     */
    default Integer getReasonCode() {
        return getQos();
    }
}
