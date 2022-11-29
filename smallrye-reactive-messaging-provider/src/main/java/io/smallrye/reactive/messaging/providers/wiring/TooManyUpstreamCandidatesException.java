package io.smallrye.reactive.messaging.providers.wiring;

import java.util.List;

public class TooManyUpstreamCandidatesException extends WiringException {
    private final Wiring.ConsumingComponent component;
    private final String incoming;
    private final List<Wiring.Component> upstreams;

    public TooManyUpstreamCandidatesException(Wiring.ConsumingComponent cc) {
        this(cc, null, null);
    }

    public TooManyUpstreamCandidatesException(Wiring.ConsumingComponent cc, String incoming, List<Wiring.Component> upstreams) {
        this.component = cc;
        this.incoming = incoming;
        this.upstreams = upstreams;
    }

    public String getMessage() {
        if (incoming != null && upstreams != null) {
            return String.format(
                    "'%s' supports a single upstream producer for channel '%s', but found %d: %s. You may want to add the '@Merge' annotation on the method.",
                    component, incoming, upstreams.size(), upstreams);
        } else {
            if (component instanceof Wiring.OutgoingConnectorComponent) {
                // outgoing connectors have a single incoming channel
                return "'mp.messaging.outgoing." + component.incomings().get(0)
                        + ".merge=true' to allow multiple upstreams.";
            } else if (component instanceof Wiring.ProcessorMediatorComponent
                    || component instanceof Wiring.SubscriberMediatorComponent) {
                return String.format(
                        "'%s' supports a single upstream producer, but found %d: %s. You may want to add the '@Merge' annotation on the method.",
                        component, component.upstreams().size(), component.upstreams());
            } else {
                return String.format(
                        "'%s' supports a single upstream producer, but found %d: %s.",
                        component, component.upstreams().size(), component.upstreams());
            }
        }
    }
}
