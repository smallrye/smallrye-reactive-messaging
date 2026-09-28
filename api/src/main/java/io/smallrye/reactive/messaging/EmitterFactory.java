package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.config.Config;

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

    /**
     * Create emitter implementation instance with access to the outgoing channel configuration.
     * <p>
     * The channel configuration is provided when the emitter's channel is connected to an outgoing connector.
     * Connector-specific factories can use this to detect when the channel uses a different connector
     * (e.g. a test connector) and return a fallback emitter implementation.
     *
     * @param configuration emitter configuration
     * @param defaultBufferSize default buffer size
     * @param channelConfig the outgoing channel configuration, or {@code null} if the channel
     *        is not connected to an outgoing connector
     * @return Emitter implementation
     */
    default MessagePublisherProvider<?> createEmitter(EmitterConfiguration configuration, long defaultBufferSize,
            Config channelConfig) {
        return createEmitter(configuration, defaultBufferSize);
    }

    /**
     * Checks whether the given channel configuration matches the expected connector.
     *
     * @param channelConfig the channel configuration, may be {@code null}
     * @param connectorName the expected connector name
     * @return {@code true} if the channel uses the expected connector or no config is available
     */
    static boolean isConnector(Config channelConfig, String connectorName) {
        if (channelConfig == null) {
            return true;
        }
        return channelConfig.getOptionalValue("connector", String.class)
                .map(connectorName::equals)
                .orElse(true);
    }

}
