package io.smallrye.reactive.messaging.observation;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * The observation collector is called with the new message and returns the message observation that will be used
 * to observe messages from their creation until the ack or the nack event
 * <p>
 *
 * <p>
 * The implementation of this interface must be a CDI managed bean in order to be discovered
 *
 * @param <T> the type of the observation context
 */
public interface MessageObservationCollector<T extends ObservationContext> {

    /**
     * Initialize observation for the given channel
     * If {@code null} is returned the observation for the given channel is disabled
     *
     * @param channel the channel of the message
     * @param incoming whether the channel is incoming or outgoing
     * @param emitter whether the channel is an emitter
     * @return the observation context
     */
    default T initObservation(String channel, boolean incoming, boolean emitter) {
        // enabled by default
        return (T) ObservationContext.DEFAULT;
    }

    /**
     * Returns a new {@link MessageObservation} object on which to collect the message processing events.
     * If {@link #initObservation(String, boolean, boolean)} is implemented,
     * the {@link ObservationContext} object returned from that method will be passed to this method.
     * If not it is called with {@link ObservationContext#DEFAULT} and should be ignored.
     *
     * @param channel the channel of the message
     * @param message the message
     * @param observationContext the observation context
     * @return the message observation
     */
    MessageObservation onNewMessage(String channel, Message<?> message, T observationContext);

}
