package io.smallrye.reactive.messaging;

import java.util.List;

import jakarta.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;

/**
 * SPI to allow extension of subscription targets (Multi) included in the final graph.
 *
 * {@code SubscriberDecorator}s are invoked higher priority first (from the least value to the greatest).
 *
 * The decorator priority is obtained with the {@link #getPriority()} method.
 * The default priority is {@link #DEFAULT_PRIORITY}.
 */
@Experimental("SmallRye only feature")
public interface SubscriberDecorator extends Prioritized {

    /**
     * Default priority
     */
    int DEFAULT_PRIORITY = 1000;

    /**
     * Decorate a Multi
     *
     * @param toBeSubscribed the multi to decorate which will be subscribed by this channel
     * @param channelName the list of channel names from which this subscriber consumes
     * @param isConnector {@code true} if decorated channel is connector
     * @return the extended multi
     */
    Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> toBeSubscribed, List<String> channelName,
            boolean isConnector);

    @Override
    default int getPriority() {
        return DEFAULT_PRIORITY;
    }

}
