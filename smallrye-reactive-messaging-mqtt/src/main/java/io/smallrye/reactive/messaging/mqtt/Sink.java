package io.smallrye.reactive.messaging.mqtt;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

public interface Sink {
    public SubscriberBuilder<? extends Message<?>, Void> getSink();

    public boolean isReady();
}
