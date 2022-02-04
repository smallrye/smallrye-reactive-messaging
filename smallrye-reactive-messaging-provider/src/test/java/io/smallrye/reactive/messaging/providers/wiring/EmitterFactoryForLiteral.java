package io.smallrye.reactive.messaging.providers.wiring;

import javax.enterprise.util.AnnotationLiteral;

import org.eclipse.microprofile.reactive.messaging.Emitter;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;

public final class EmitterFactoryForLiteral extends AnnotationLiteral<EmitterFactoryFor> implements EmitterFactoryFor {

    private final Class<?> value;

    public static EmitterFactoryFor of(Class<?> value) {
        return new EmitterFactoryForLiteral(value);
    }

    private EmitterFactoryForLiteral(Class<?> value) {
        this.value = value;
    }

    @Override
    public Class<?> value() {
        return value;
    }

    public static EmitterFactoryFor EMITTER = of(Emitter.class);
    public static EmitterFactoryFor MUTINY_EMITTER = of(MutinyEmitter.class);
    public static EmitterFactoryFor LEGACY_EMITTER = of(io.smallrye.reactive.messaging.annotations.Emitter.class);
}
