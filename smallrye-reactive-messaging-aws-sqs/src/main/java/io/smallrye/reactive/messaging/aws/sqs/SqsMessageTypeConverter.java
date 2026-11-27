package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging.log;

import java.lang.reflect.Type;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

/**
 * Restores the SQS message type ({@link SqsMessage}) for methods consuming it, when the message lost it on the way:
 * features that intercept messages (interceptors, observation, graceful shutdown tracking) rebuild messages, keeping
 * the payload, the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and the SQS message from the
 * {@link SqsIncomingMetadata} of that metadata. It delegates acknowledgement and negative acknowledgement to the
 * intercepted message, so the interception keeps working.
 */
@ApplicationScoped
public class SqsMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.getRawTypeIfParameterized(target).equals(SqsMessage.class)
                && !(in instanceof SqsMessage)
                && in.getMetadata(SqsIncomingMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        if (warned.compareAndSet(false, true)) {
            log.restoringSqsMessageType(TypeUtils.getRawTypeIfParameterized(target).getTypeName());
        }
        return new RestoredSqsMessage<>(in);
    }

    /**
     * An SQS message restored from an intercepted message: payload and metadata come from that message, the SQS
     * message from its SQS metadata, and acknowledgement is delegated to it.
     */
    static class RestoredSqsMessage<T> extends SqsMessage<T> {

        private final Message<T> delegate;

        @SuppressWarnings("unchecked")
        RestoredSqsMessage(Message<?> delegate) {
            super(delegate.getMetadata(SqsIncomingMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No SQS message metadata"))
                    .getMessage(), delegate.getMetadata());
            this.delegate = (Message<T>) delegate;
        }

        @Override
        public T getPayload() {
            return delegate.getPayload();
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            return delegate.ack(metadata);
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return delegate.nack(reason, metadata);
        }

        @Override
        public void injectMetadata(Object metadataObject) {
            super.injectMetadata(metadataObject);
            // Keep the intercepted message, which acknowledges, consistent with the restored one.
            if (delegate instanceof MetadataInjectableMessage<?> injectable) {
                injectable.injectMetadata(metadataObject);
            }
        }
    }
}
