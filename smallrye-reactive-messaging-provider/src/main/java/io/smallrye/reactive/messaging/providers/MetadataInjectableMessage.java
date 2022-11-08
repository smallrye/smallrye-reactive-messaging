package io.smallrye.reactive.messaging.providers;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Message type which enables injecting new metadata without creating a new {@link Message} instance
 *
 * @param <T> type of payload
 */
public interface MetadataInjectableMessage<T> extends Message<T> {

    /**
     * Inject the given metadata object
     * 
     * @param metadataObject metadata object
     */
    void injectMetadata(Object metadataObject);
}
