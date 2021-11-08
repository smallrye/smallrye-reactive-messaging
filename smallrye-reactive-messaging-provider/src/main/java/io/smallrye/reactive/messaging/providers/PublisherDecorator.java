package io.smallrye.reactive.messaging.providers;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;

/**
 * SPI to allow extension of publishers (Multi) included in the final graph
 */
public interface PublisherDecorator {

    /**
     * Decorate a Multi
     *
     * @param publisher the multi to decorate
     * @param channelName the name of the channel to which this publisher publishes
     * @return the extended multi
     */
    Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName);

}
