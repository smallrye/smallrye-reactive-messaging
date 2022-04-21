package io.smallrye.reactive.messaging.providers.extension;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Emitter;

import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;

@EmitterFactoryFor(Emitter.class)
@ApplicationScoped
// Wildcard parameterized type is not a legal managed bean
public class EmitterFactoryImpl implements EmitterFactory<EmitterImpl<Object>> {
    @Override
    public EmitterImpl<Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return new EmitterImpl<>(configuration, defaultBufferSize);
    }
}
