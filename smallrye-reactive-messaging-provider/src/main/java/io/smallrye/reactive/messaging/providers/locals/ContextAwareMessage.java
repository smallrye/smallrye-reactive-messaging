package io.smallrye.reactive.messaging.providers.locals;

import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.common.constraint.Nullable;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Context;

public interface ContextAwareMessage<T> extends Message<T> {

    static @Nullable LocalContextMetadata captureLocalContextMetadata() {
        Context duplicatedContext = VertxContext.createNewDuplicatedContext();
        if (duplicatedContext == null) {
            return null;
        } else {
            return new LocalContextMetadata(duplicatedContext);
        }
    }

    static <T> ContextAwareMessage<T> of(T payload) {
        LocalContextMetadata contextMetadata = captureLocalContextMetadata();
        Metadata metadata = contextMetadata == null ? Metadata.empty() : Metadata.of(contextMetadata);
        return new ContextAwareMessage<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return metadata;
            }
        };
    }

    static <T> Message<T> withContextMetadata(Message<T> message) {
        LocalContextMetadata contextMetadata = captureLocalContextMetadata();
        return contextMetadata == null ? message : message.addMetadata(contextMetadata);
    }

    @CheckReturnValue
    static Metadata captureContextMetadata(Metadata metadata) {
        LocalContextMetadata localContextMetadata = captureLocalContextMetadata();
        if (localContextMetadata == null) {
            // nothing to do not on vert.x context
            return metadata;
        } else {
            return metadata.with(localContextMetadata);
        }
    }

    @CheckReturnValue
    static Metadata captureContextMetadata(Object... metadata) {
        return captureContextMetadata(Metadata.of(metadata));
    }

    @CheckReturnValue
    static Metadata captureContextMetadata(Iterable<Object> metadata) {
        return captureContextMetadata(Metadata.from(metadata));
    }

    default Optional<LocalContextMetadata> getContextMetadata() {
        return getMetadata().get(LocalContextMetadata.class);
    }
}
