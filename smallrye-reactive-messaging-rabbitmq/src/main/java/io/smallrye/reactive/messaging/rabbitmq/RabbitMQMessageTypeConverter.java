package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

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
 * Restores the RabbitMQ message type ({@link IncomingRabbitMQMessage}) for methods consuming it, when the message lost
 * it on the way: features that intercept messages (interceptors, observation, graceful shutdown tracking) rebuild
 * messages, keeping the payload, the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and the RabbitMQ envelope,
 * properties and body from the {@link IncomingRabbitMQMetadata} of that metadata. It delegates acknowledgement and
 * negative acknowledgement to the intercepted message, so the interception keeps working.
 */
@ApplicationScoped
public class RabbitMQMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.getRawTypeIfParameterized(target).equals(IncomingRabbitMQMessage.class)
                && !(in instanceof IncomingRabbitMQMessage)
                && in.getMetadata(IncomingRabbitMQMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        if (warned.compareAndSet(false, true)) {
            log.restoringRabbitMQMessageType(TypeUtils.getRawTypeIfParameterized(target).getTypeName());
        }
        return new RestoredRabbitMQMessage<>(in);
    }

    /**
     * A RabbitMQ message restored from an intercepted message: payload and metadata come from that message, the
     * RabbitMQ envelope and properties from its RabbitMQ metadata, and acknowledgement is delegated to it.
     */
    static class RestoredRabbitMQMessage<T> extends IncomingRabbitMQMessage<T> {

        private final Message<T> delegate;
        private final IncomingRabbitMQMetadata rabbitMQMetadata;
        private volatile Metadata metadata;

        @SuppressWarnings("unchecked")
        RestoredRabbitMQMessage(Message<?> delegate) {
            this(delegate, delegate.getMetadata(IncomingRabbitMQMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No RabbitMQ message metadata")));
        }

        @SuppressWarnings("unchecked")
        private RestoredRabbitMQMessage(Message<?> delegate, IncomingRabbitMQMetadata rabbitMQMetadata) {
            // The payload comes from the intercepted message: no conversion of the body, no handlers.
            super(rabbitMQMetadata.getEnvelope(), rabbitMQMetadata.getProperties(), rabbitMQMetadata.getBody(),
                    body -> null, null, null, null, rabbitMQMetadata.getContentTypeOverride());
            this.delegate = (Message<T>) delegate;
            this.rabbitMQMetadata = rabbitMQMetadata;
            this.metadata = delegate.getMetadata();
        }

        @Override
        public T getPayload() {
            return delegate.getPayload();
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }

        @Override
        public IncomingRabbitMQMetadata getRabbitMQMetadata() {
            return rabbitMQMetadata;
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
            this.metadata = this.metadata.with(metadataObject);
            // Keep the intercepted message, which acknowledges, consistent with the restored one.
            if (delegate instanceof MetadataInjectableMessage<?> injectable) {
                injectable.injectMetadata(metadataObject);
            }
        }
    }
}
