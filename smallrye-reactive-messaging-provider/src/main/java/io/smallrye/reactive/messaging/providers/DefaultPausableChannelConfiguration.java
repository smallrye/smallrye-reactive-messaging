package io.smallrye.reactive.messaging.providers;

import io.smallrye.reactive.messaging.PausableChannelConfiguration;

/**
 * Default implementation of {@link PausableChannelConfiguration}.
 */
public record DefaultPausableChannelConfiguration(String name, boolean initiallyPaused, boolean lateSubscription,
        Integer bufferSize, PausableBufferStrategy bufferStrategy)
        implements
            PausableChannelConfiguration {

}
