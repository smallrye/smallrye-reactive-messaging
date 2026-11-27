package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

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
 * Restores the JMS message type ({@link IncomingJmsMessage}) for methods consuming it, when the message lost it on
 * the way: features that intercept messages (interceptors, observation, graceful shutdown tracking) rebuild messages,
 * keeping the payload, the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and the JMS message from the
 * {@link IncomingJmsMessageMetadata} of that metadata, so {@link IncomingJmsMessage#unwrap(Class)}, the transaction
 * metadata ({@link JmsSessionContext}, {@link JmsXaTransactionMetadata}) and the message context keep working. It
 * delegates acknowledgement and negative acknowledgement to the intercepted message, so the interception keeps
 * working, and the intercepted message acknowledges the original one, which commits or rolls back the transaction.
 */
@ApplicationScoped
public class JmsMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.getRawTypeIfParameterized(target).equals(IncomingJmsMessage.class)
                && !(in instanceof IncomingJmsMessage)
                && in.getMetadata(IncomingJmsMessageMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        if (warned.compareAndSet(false, true)) {
            log.restoringJmsMessageType(TypeUtils.getRawTypeIfParameterized(target).getTypeName());
        }
        return new RestoredIncomingJmsMessage<>(in);
    }

    /**
     * A JMS message restored from an intercepted message: payload and metadata come from that message, the JMS message
     * from its JMS metadata, and acknowledgement is delegated to it.
     */
    static class RestoredIncomingJmsMessage<T> extends IncomingJmsMessage<T> {

        private final Message<T> delegate;

        @SuppressWarnings("unchecked")
        RestoredIncomingJmsMessage(Message<?> delegate) {
            super(delegate.getMetadata(IncomingJmsMessageMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No JMS message metadata")),
                    delegate.getMetadata());
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
