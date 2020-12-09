package io.smallrye.reactive.messaging.wiring;

import java.util.Set;

public class OpenGraphException extends WiringException {
    private final Set<Wiring.Component> resolved;
    private final Set<Wiring.ConsumingComponent> unresolved;

    public OpenGraphException(Set<Wiring.Component> components,
            Set<Wiring.ConsumingComponent> unresolved) {
        this.resolved = components;
        this.unresolved = unresolved;
    }

    @Override
    public String getMessage() {
        StringBuffer message = new StringBuffer(
                "Some components are not connected to either downstream consumers or upstream producers:\n");
        this.resolved.stream().filter(component -> !component.isDownstreamResolved())
                .forEach(c -> message.append("\t- ").append(c).append(" has no downstream\n"));
        this.unresolved.stream().filter(component -> !component.isDownstreamResolved())
                .forEach(c -> message.append("\t- ").append(c).append(" has no downstream\n"));
        this.unresolved.stream()
                .filter(c -> c.upstreams().isEmpty())
                .forEach(c -> message.append("\t- ").append(c).append(" has no upstream\n"));

        return message.toString();
    }
}
