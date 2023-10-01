package io.smallrye.reactive.messaging.aws.sqs.message;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.aws.sqs.SqsTarget;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public abstract class SqsMessage<T, M extends SqsMessageMetadata> implements ContextAwareMessage<T> {

    private final T payload;
    private SqsTarget target;
    private final M sqsMetadata;
    private final Metadata metadata;

    public SqsMessage(T payload, M sqsMetadata) {
        this.payload = payload;
        this.sqsMetadata = sqsMetadata;
        this.metadata = captureContextMetadata(sqsMetadata);
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    public SqsTarget getTarget() {
        return target;
    }

    public SqsMessage<T, M> withTarget(SqsTarget target) {
        this.target = target;
        return this;
    }

    public M getSqsMetadata() {
        return sqsMetadata;
    }
}
