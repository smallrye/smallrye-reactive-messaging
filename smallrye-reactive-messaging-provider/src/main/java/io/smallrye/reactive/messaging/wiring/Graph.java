package io.smallrye.reactive.messaging.wiring;

import static io.smallrye.reactive.messaging.extension.MediatorManager.STRICT_MODE_PROPERTY;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;

import io.smallrye.reactive.messaging.ChannelRegistry;

public class Graph {

    private static final Logger LOGGER = Logger.getLogger("Reactive Messaging Graph");

    private final Set<Wiring.Component> resolved;
    private final Set<Wiring.ConsumingComponent> unresolved;
    private final boolean isClosed;
    private final Set<Wiring.PublishingComponent> inbound;
    private final Set<Wiring.ConsumingComponent> outbound;
    private final List<Exception> errors = new ArrayList<>();

    public Graph(Set<Wiring.Component> resolved, Set<Wiring.ConsumingComponent> unresolved) {
        this.resolved = resolved;
        this.unresolved = unresolved;
        this.isClosed = computeClosure();

        boolean strict = Boolean.parseBoolean(System.getProperty(STRICT_MODE_PROPERTY, "false"));
        if (strict && !isClosed) {
            errors.add(new NotClosedGraph(this.resolved, unresolved));
        }
        if (!strict && !isClosed) {
            StringBuffer message = new StringBuffer(
                    "Some components are not connected to either downstream consumers or upstream producers:\n");
            this.resolved.stream().filter(component -> !component.isDownstreamResolved())
                    .forEach(c -> message.append("\t- ").append(c).append(" has no downstream\n"));
            this.unresolved.stream().filter(component -> !component.isDownstreamResolved())
                    .forEach(c -> message.append("\t- ").append(c).append(" has no downstream\n"));
            this.unresolved.stream()
                    .filter(c -> !c.isUpstreamResolved())
                    .forEach(c -> {
                        if (c.upstreams().isEmpty()) {
                            message.append("\t- ").append(c).append(" has no upstream\n");
                        } else {
                            message.append("\t- ").append(c).append(" does not have all its requested upstreams: ")
                                    .append(c.upstreams()).append("\n");
                        }
                    });
            LOGGER.warn(message);
        }

        this.inbound = this.resolved.stream()
                .filter(c -> c.isUpstreamResolved() && c.upstreams().isEmpty() && isDownstreamFullyResolved(c))
                .map(c -> (Wiring.PublishingComponent) c)
                .collect(Collectors.toSet());
        // Verify that the full chain is ok
        this.outbound = this.resolved.stream()
                .filter(c -> c.isDownstreamResolved() && c.downstreams().isEmpty() && isUpstreamFullyResolved(c))
                .map(c -> (Wiring.ConsumingComponent) c)
                .collect(Collectors.toSet());

        // Validate
        this.resolved.forEach(c -> {
            try {
                c.validate();
            } catch (WiringException e) {
                errors.add(e);
            }
        });
    }

    public List<Exception> getWiringErrors() {
        return errors;
    }

    public void materialize(ChannelRegistry registry) {
        Set<Wiring.Component> materialized = new HashSet<>();
        List<Wiring.Component> current = new ArrayList<>(inbound);
        while (!current.isEmpty()) {
            List<Wiring.Component> downstreams = new ArrayList<>();
            for (Wiring.Component cmp : current) {
                if (!materialized.contains(cmp) && allUpstreamsMaterialized(cmp, materialized)) {
                    cmp.materialize(registry);
                    downstreams.addAll(cmp.downstreams());
                    materialized.add(cmp);
                }
            }
            current.removeAll(materialized);
            current.addAll(downstreams);
        }
    }

    private boolean allUpstreamsMaterialized(Wiring.Component cmp, Set<Wiring.Component> materialized) {
        for (Wiring.Component upstream : cmp.upstreams()) {
            if (!materialized.contains(upstream)) {
                return false;
            }
        }
        return true;
    }

    public Set<Wiring.Component> getResolvedComponents() {
        return resolved;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public Set<Wiring.PublishingComponent> getInbound() {
        return inbound;
    }

    public Set<Wiring.ConsumingComponent> getOutbound() {
        return outbound;
    }

    private boolean isUpstreamFullyResolved(Wiring.Component component) {
        if (!component.isUpstreamResolved()) {
            return false;
        }
        for (Wiring.Component upstream : component.upstreams()) {
            if (!isUpstreamFullyResolved(upstream)) {
                return false;
            }
        }
        return true;
    }

    private boolean isDownstreamFullyResolved(Wiring.Component component) {
        if (!component.isDownstreamResolved()) {
            return false;
        }
        for (Wiring.Component downstream : component.downstreams()) {
            if (!isDownstreamFullyResolved(downstream)) {
                return false;
            }
        }
        return true;
    }

    public Set<Wiring.ConsumingComponent> getUnresolvedComponents() {
        return unresolved;
    }

    public boolean hasWiringErrors() {
        return !errors.isEmpty();
    }

    private boolean computeClosure() {
        if (!unresolved.isEmpty()) {
            return false;
        }

        for (Wiring.Component component : resolved) {
            if (!component.isUpstreamResolved()) {
                return false;
            }
            // One test from the TCK deploys an unused connector.
            if (!(component instanceof Wiring.InboundConnectorComponent) && !component.isDownstreamResolved()) {
                return false;
            } else if (component instanceof Wiring.InboundConnectorComponent && !component.isDownstreamResolved()) {
                LOGGER.warn("The connector " + component + " has no downstreams");
            }
        }
        return true;
    }
}
