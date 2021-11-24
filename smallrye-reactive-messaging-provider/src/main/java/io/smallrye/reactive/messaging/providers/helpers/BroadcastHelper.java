package io.smallrye.reactive.messaging.providers.helpers;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;

public class BroadcastHelper {

    private BroadcastHelper() {
        // Avoid direct instantiation.
    }

    /**
     * <p>
     * Wraps an existing {@code Publisher} for broadcasting.
     * </p>
     *
     * @param publisher The publisher to be wrapped
     * @param numberOfSubscriberBeforeConnecting Number of subscribers that must be present before broadcast occurs.
     *        A value of 0 means any number of subscribers will trigger the broadcast.
     * @return The wrapped {@code Publisher} in a new {@code PublisherBuilder}
     */
    public static Multi<? extends Message<?>> broadcastPublisher(Multi<? extends Message<?>> publisher,
            int numberOfSubscriberBeforeConnecting) {
        if (numberOfSubscriberBeforeConnecting != 0) {
            return publisher
                    .broadcast().toAtLeast(numberOfSubscriberBeforeConnecting);
        } else {
            return publisher.broadcast().toAllSubscribers();
        }
    }
}
