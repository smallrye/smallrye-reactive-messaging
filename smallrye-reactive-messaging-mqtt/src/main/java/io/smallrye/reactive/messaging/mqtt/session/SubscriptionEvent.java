package io.smallrye.reactive.messaging.mqtt.session;

public interface SubscriptionEvent {
    Integer getQos();

    SubscriptionState getSubscriptionState();

    String getTopic();
}
