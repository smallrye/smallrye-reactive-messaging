package io.smallrye.reactive.messaging.providers.wiring;

public class UnsatisfiedBroadcastException extends WiringException {
    private final Wiring.PublishingComponent component;

    public UnsatisfiedBroadcastException(Wiring.PublishingComponent pc) {
        this.component = pc;
    }

    public String getMessage() {
        return String.format(
                "'%s' requires %d downstream consumers, but found %d: %s",
                component, component.getRequiredNumberOfSubscribers(), component.downstreams().size(), component.downstreams());
    }
}
