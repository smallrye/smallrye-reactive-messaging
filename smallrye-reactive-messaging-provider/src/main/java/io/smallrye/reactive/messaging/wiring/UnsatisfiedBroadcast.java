package io.smallrye.reactive.messaging.wiring;

public class UnsatisfiedBroadcast extends WiringException {
    private final Wiring.PublishingComponent component;

    public UnsatisfiedBroadcast(Wiring.PublishingComponent pc) {
        this.component = pc;
    }

    public String getMessage() {
        return String.format(
                "'%s' requires %d downstream consumers, but found %d: %s",
                component, component.getRequiredNumberOfSubscribers(), component.downstreams().size(), component.downstreams());
    }
}
