package io.smallrye.reactive.messaging.memory;

/**
 * Allows interacting with an in-memory source.
 *
 * @param <T> the type of payload or message.
 * @deprecated Use {@link TestIncoming} instead.
 */
@Deprecated(forRemoval = true)
public interface InMemorySource<T> extends TestIncoming<T> {

    /**
     * @deprecated Use {@link #deliver(Object)} instead.
     */
    @Deprecated(forRemoval = true)
    InMemorySource<T> send(T messageOrPayload);

    @Override
    InMemorySource<T> runOnVertxContext(boolean runOnVertxContext);
}
