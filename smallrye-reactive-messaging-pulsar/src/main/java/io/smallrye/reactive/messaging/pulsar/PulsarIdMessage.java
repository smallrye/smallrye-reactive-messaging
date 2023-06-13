package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.MessageId;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public interface PulsarIdMessage<T> extends ContextAwareMessage<T> {

    MessageId getMessageId();
}
