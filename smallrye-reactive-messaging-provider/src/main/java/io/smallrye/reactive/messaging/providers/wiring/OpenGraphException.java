package io.smallrye.reactive.messaging.providers.wiring;

import java.util.Set;

public class OpenGraphException extends WiringException {
    public OpenGraphException(final String message) {
        super(message);
    }

    public static OpenGraphException openGraphException(Set<Wiring.Component> resolved,
            Set<Wiring.ConsumingComponent> unresolved) {
        StringBuffer message = new StringBuffer(
                "Some components are not connected to either downstream consumers or upstream producers:\n");
        resolved.stream().filter(component -> !component.isDownstreamResolved())
                .forEach(c -> message.append("\t- ").append(c).append(" has no downstream\n"));
        unresolved.stream().filter(component -> !component.isDownstreamResolved())
                .forEach(c -> message.append("\t- ").append(c).append(" has no downstream\n"));
        unresolved.stream()
                .filter(c -> c.upstreams().isEmpty())
                .forEach(c -> message.append("\t- ").append(c).append(" has no upstream\n"));

        return new OpenGraphException(message.toString());
    }
}
