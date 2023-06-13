package io.smallrye.reactive.messaging.pulsar;

import java.util.List;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public interface PulsarBatchMessage<T> extends ContextAwareMessage<List<T>>, Iterable<PulsarMessage<T>> {
    /**
     * @return list of messages contained in this message batch
     */
    List<PulsarMessage<T>> getMessages();

}
