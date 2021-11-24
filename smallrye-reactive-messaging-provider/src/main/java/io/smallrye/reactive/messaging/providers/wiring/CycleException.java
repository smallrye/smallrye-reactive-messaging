package io.smallrye.reactive.messaging.providers.wiring;

public class CycleException extends WiringException {
    private final Wiring.Component component;
    private final Wiring.Component downstream;

    public CycleException(Wiring.Component component, Wiring.Component downstream) {
        this.component = component;
        this.downstream = downstream;
    }

    @Override
    public String getMessage() {
        return "Cycle detected between " + component + " and a downstream component " + downstream;
    }
}
