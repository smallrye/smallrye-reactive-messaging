package io.smallrye.reactive.messaging.providers.extension;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import jakarta.enterprise.inject.spi.Bean;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.providers.DefaultMediatorConfiguration;

class CollectedMediatorMetadata {

    private final List<MediatorConfiguration> mediators = new ArrayList<>();

    boolean strict = false;

    void strict() {
        strict = true;
    }

    void add(Method method, Bean<?> bean) {
        mediators.add(createMediatorConfiguration(method, bean));
    }

    private MediatorConfiguration createMediatorConfiguration(Method met, Bean<?> bean) {
        DefaultMediatorConfiguration configuration = new DefaultMediatorConfiguration(met, bean);
        if (strict) {
            configuration.strict();
        }

        Incomings incomings = met.getAnnotation(Incomings.class);
        Incoming incoming = met.getAnnotation(Incoming.class);
        Outgoing outgoing = met.getAnnotation(Outgoing.class);
        Blocking blocking = met.getAnnotation(Blocking.class);
        if (incomings != null) {
            configuration.compute(incomings, outgoing, blocking);
        } else if (incoming != null) {
            configuration.compute(Collections.singletonList(incoming), outgoing, blocking);
        } else {
            configuration.compute(Collections.emptyList(), outgoing, blocking);
        }
        return configuration;
    }

    void addAll(Collection<? extends MediatorConfiguration> mediators) {
        this.mediators.addAll(mediators);
    }

    List<MediatorConfiguration> mediators() {
        return mediators;
    }
}
