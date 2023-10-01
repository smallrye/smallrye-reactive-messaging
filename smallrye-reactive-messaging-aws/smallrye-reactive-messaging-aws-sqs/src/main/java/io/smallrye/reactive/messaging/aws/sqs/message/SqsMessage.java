package io.smallrye.reactive.messaging.aws.sqs.message;

import io.smallrye.reactive.messaging.aws.sqs.SqsTarget;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public abstract class SqsMessage<T, M extends SqsMessageMetadata> implements ContextAwareMessage<T> {

    private SqsTarget target;
    private final M metadata;

    public SqsMessage(M metadata) {
        this.metadata = metadata;
    }

    public SqsTarget getTarget() {
        return target;
    }

    public SqsMessage<T, M> withTarget(SqsTarget target) {
        this.target = target;
        return this;
    }

    public M getSqsMetadata() {
        return metadata;
    }
}
