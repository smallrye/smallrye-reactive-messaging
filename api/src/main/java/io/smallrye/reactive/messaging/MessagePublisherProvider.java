package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

/**
 * Framework-facing interface for the Emitter implementations.
 *
 * Via this interface Emitter implementations provide emitted {@link Message}s as a Reactive Streams {@link Publisher}.
 *
 *
 * @param <T> message payload type.
 */
public interface MessagePublisherProvider<T> {

    Publisher<Message<? extends T>> getPublisher();
}
