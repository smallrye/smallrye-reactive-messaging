package io.smallrye.reactive.messaging;

import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;

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
