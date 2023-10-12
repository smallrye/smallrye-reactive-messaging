package io.smallrye.reactive.messaging.observation;

/**
 * The per-channel context of the Message observation.
 * It is created at the observation initialization by-channel and included at each message observation calls.
 */
public interface ObservationContext {

    /**
     * Default no-op observation context
     */
    ObservationContext DEFAULT = observation -> {

    };

    /**
     * Called after observation is completed.
     *
     * @param observation the completed message observation
     */
    void complete(MessageObservation observation);
}
