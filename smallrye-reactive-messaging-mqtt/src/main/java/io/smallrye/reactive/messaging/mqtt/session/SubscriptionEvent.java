package io.smallrye.reactive.messaging.mqtt.session;

import io.vertx.codegen.annotations.VertxGen;

@VertxGen
public interface SubscriptionEvent {
    Integer getQos();

    SubscriptionState getSubscriptionState();

    String getTopic();
}
