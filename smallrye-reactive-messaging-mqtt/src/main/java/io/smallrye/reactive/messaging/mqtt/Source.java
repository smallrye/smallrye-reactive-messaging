package io.smallrye.reactive.messaging.mqtt;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

public interface Source {

    PublisherBuilder<MqttMessage<?>> getSource();

    boolean isSubscribed();
}
