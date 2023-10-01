package io.smallrye.reactive.messaging.aws.sqs.message;

import io.smallrye.reactive.messaging.aws.sqs.Target;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public abstract class SqsMessage<T, M extends SqsMessageMetadata> implements ContextAwareMessage<T> {

    private Target target;
    private final M metadata;

    public SqsMessage(M metadata) {
        this.metadata = metadata;
    }

    public Target getTarget() {
        return target;
    }

    public SqsMessage<T, M> withTarget(Target target) {
        this.target = target;
        return this;
    }

    public M getSqsMetadata() {
        return metadata;
    }
}
