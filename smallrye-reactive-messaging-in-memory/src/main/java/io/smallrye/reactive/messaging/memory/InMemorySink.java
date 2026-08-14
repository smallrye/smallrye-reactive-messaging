package io.smallrye.reactive.messaging.memory;

import java.util.List;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Allows interacting with an in-memory sink.
 *
 * @param <T> the type payload expected in the received messages.
 * @deprecated Use {@link TestOutgoing} instead.
 */
@Deprecated(forRemoval = true)
public interface InMemorySink<T> extends TestOutgoing<T> {

    /**
     * @deprecated Use {@link #sent()} instead.
     */
    @Deprecated(forRemoval = true)
    List<? extends Message<T>> received();
}
