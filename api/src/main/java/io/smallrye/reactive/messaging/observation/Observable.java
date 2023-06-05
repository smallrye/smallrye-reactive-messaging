package io.smallrye.reactive.messaging.observation;

/**
 * Indicates that a connector can contribute to the observations
 */
public interface Observable {

    /**
     * Gets the {@link ReactiveMessagingObservation} on which to report the message processing events.
     *
     * @param observation the observation object, cannot be {@code null}
     */
    void setReactiveMessageObservation(ReactiveMessagingObservation observation);

}
