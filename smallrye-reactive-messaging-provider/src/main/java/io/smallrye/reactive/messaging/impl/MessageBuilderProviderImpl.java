package io.smallrye.reactive.messaging.impl;

import io.smallrye.reactive.messaging.MessageBuilder;
import org.eclipse.microprofile.reactive.messaging.spi.MessageBuilderProvider;

public class MessageBuilderProviderImpl extends MessageBuilderProvider {

    @Override
    @SuppressWarnings("rawtypes")
    public <T> MessageBuilder<T> newBuilder() {
        return new MessageBuilderImpl<>();
    }
}
