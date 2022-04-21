package io.smallrye.reactive.messaging.providers.extension;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;

@EmitterFactoryFor(Emitter.class)
@ApplicationScoped
// Wildcard parameterized type is not a legal managed bean
public class LegacyEmitterFactoryImpl implements EmitterFactory<LegacyEmitterImpl<Object>> {
    @Override
    public LegacyEmitterImpl<Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return new LegacyEmitterImpl<>(new EmitterImpl<>(configuration, defaultBufferSize));
    }
}
