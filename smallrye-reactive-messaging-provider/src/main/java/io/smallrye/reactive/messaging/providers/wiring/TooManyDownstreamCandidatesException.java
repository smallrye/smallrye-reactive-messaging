package io.smallrye.reactive.messaging.providers.wiring;

public class TooManyDownstreamCandidatesException extends WiringException {
    private final Wiring.PublishingComponent component;

    public TooManyDownstreamCandidatesException(Wiring.PublishingComponent pc) {
        this.component = pc;
    }

    public String getMessage() {
        return String.format(
                "'%s' supports a single downstream consumer, but found %d: %s. You may want to enable broadcast using %s",
                component, component.downstreams().size(), component.downstreams(), getHint());
    }

    private String getHint() {
        if (component instanceof Wiring.InboundConnectorComponent) {
            return "'mp.messaging.incoming." + component.getOutgoingChannel()
                    + ".broadcast=true' + to allow multiple downstreams.";
        } else if (component instanceof Wiring.EmitterComponent) {
            return "'@Broadcast' on the injected emitter field.";
        } else if (component instanceof Wiring.PublisherMediatorComponent
                || component instanceof Wiring.ProcessorMediatorComponent) {
            return "'@Broadcast' on the method " + component + ".";
        }
        throw new IllegalStateException("Unable to provide the broadcast hint " + component);
    }
}
