package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

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
 * Restores the AMQP message type ({@link AmqpMessage}) for methods consuming it, when the message lost it on the way:
 * features that intercept messages (interceptors, observation, graceful shutdown tracking) rebuild messages, keeping
 * the payload, the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and the AMQP message from the
 * {@link IncomingAmqpMetadata} of that metadata. It delegates acknowledgement and negative acknowledgement to the
 * intercepted message, so the interception keeps working.
 */
@ApplicationScoped
public class AmqpMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.getRawTypeIfParameterized(target).equals(AmqpMessage.class)
                && !(in instanceof AmqpMessage)
                && in.getMetadata(IncomingAmqpMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        if (warned.compareAndSet(false, true)) {
            log.restoringAmqpMessageType(TypeUtils.getRawTypeIfParameterized(target).getTypeName());
        }
        return new RestoredAmqpMessage<>(in);
    }

    /**
     * An AMQP message restored from an intercepted message: payload and metadata come from that message, the AMQP
     * message from its AMQP metadata, and acknowledgement is delegated to it.
     */
    static class RestoredAmqpMessage<T> extends AmqpMessage<T> {

        private final Message<T> delegate;

        @SuppressWarnings("unchecked")
        RestoredAmqpMessage(Message<?> delegate) {
            super(delegate.getMetadata(IncomingAmqpMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No AMQP message metadata"))
                    .getMessage(), null, null, false, false);
            this.delegate = (Message<T>) delegate;
            this.metadata = delegate.getMetadata();
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
        public synchronized void injectMetadata(Object metadataObject) {
            super.injectMetadata(metadataObject);
            // Keep the intercepted message, which acknowledges, consistent with the restored one.
            if (delegate instanceof MetadataInjectableMessage<?> injectable) {
                injectable.injectMetadata(metadataObject);
            }
        }
    }
}
