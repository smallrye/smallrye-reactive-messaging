package io.smallrye.reactive.messaging.observation;

/**
 * An object encapsulating a {@link ReactiveMessagingObservation.MessageObservation} and carried as message metadata.
 * We cannot use {@link ReactiveMessagingObservation.MessageObservation} directly in the metadata as we need to allow
 * subclasses.
 */
public class ObservationMetadata {

    private final ReactiveMessagingObservation.MessageObservation observation;

    public ObservationMetadata(ReactiveMessagingObservation.MessageObservation observation) {
        this.observation = observation;
    }

    public ReactiveMessagingObservation.MessageObservation observation() {
        return observation;
    }

}
