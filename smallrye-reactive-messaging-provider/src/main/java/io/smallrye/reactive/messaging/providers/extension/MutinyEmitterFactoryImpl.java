package io.smallrye.reactive.messaging.providers.extension;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;

@EmitterFactoryFor(MutinyEmitter.class)
@ApplicationScoped
// Wildcard parameterized type is not a legal managed bean
public class MutinyEmitterFactoryImpl implements EmitterFactory<MutinyEmitterImpl<Object>> {
    @Override
    public MutinyEmitterImpl<Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return new MutinyEmitterImpl<>(configuration, defaultBufferSize);
    }
}
