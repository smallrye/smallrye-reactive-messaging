package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

/**
 * SPI to allow extension of publishers included in the final graph
 */
public interface PublisherDecorator {

    /**
     * Decorate a publisher
     * 
     * @param publisher the publisher to decorate
     * @param channelName the name of the channel to which this publisher publishes
     * @return the extended publisher
     */
    public PublisherBuilder<? extends Message> decorate(PublisherBuilder<? extends Message> publisher, String channelName);

}
