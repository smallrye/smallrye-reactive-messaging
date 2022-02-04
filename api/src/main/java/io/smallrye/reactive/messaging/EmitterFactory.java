package io.smallrye.reactive.messaging;

/**
 * Factory for creating different Emitter implementations.
 * <p>
 * The implementation need to be provided as an {@code ApplicationScoped} bean
 * qualified with {@link io.smallrye.reactive.messaging.annotations.EmitterFactoryFor},
 * which contains the public interface of the Emitter.
 * <p>
 * Emitter implementations created by this factory are registered to {@link io.smallrye.reactive.messaging.ChannelRegistry}.
 * <p>
 * Custom implementations can provide a CDI {@code @Produces} method to make their custom Emitter interface injectable into
 * managed beans.
 *
 * @param <T> emitter implementation type, extends {@link MessagePublisherProvider}
 */
public interface EmitterFactory<T extends MessagePublisherProvider<?>> {

    /**
     * Create emitter implementation instance
     *
     * @param configuration emitter configuration
     * @param defaultBufferSize default buffer size
     * @return Emitter implementation
     */
    T createEmitter(EmitterConfiguration configuration, long defaultBufferSize);

}
