package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactionMetadata;

/**
 * Restores the Pulsar message type ({@link PulsarMessage}, {@link PulsarIdMessage}, {@link PulsarIncomingMessage},
 * {@link PulsarBatchMessage}, {@link PulsarIncomingBatchMessage}) for methods consuming it, when the message lost it on
 * the way: features that intercept messages (interceptors, observation, graceful shutdown tracking) rebuild messages,
 * keeping the payload, the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and delegates acknowledgement
 * to it, so the interception keeps working. Metadata injected into the restored message, such as the
 * {@link PulsarTransactionMetadata} injected by {@code PulsarTransactions.withTransaction(message, work)}, is
 * forwarded to the intercepted message and to the original Pulsar message, whose metadata the acknowledgement reads:
 * acknowledging the restored message inside a transaction acknowledges the Pulsar message in that transaction.
 * <p>
 * Consuming Pulsar message types is deprecated: this is a compatibility measure, and it logs a warning.
 */
@ApplicationScoped
public class PulsarMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        Type raw = TypeUtils.getRawTypeIfParameterized(target);
        if (raw instanceof Class<?> type && type.isInstance(in)) {
            return false;
        }
        if (isMessageType(raw)) {
            return !(in instanceof PulsarIncomingMessage)
                    && in.getMetadata(PulsarIncomingMessageMetadata.class).isPresent();
        }
        if (isBatchType(raw)) {
            return !(in instanceof PulsarIncomingBatchMessage)
                    && in.getMetadata(PulsarIncomingBatchMessageMetadata.class).isPresent();
        }
        return false;
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        Type raw = TypeUtils.getRawTypeIfParameterized(target);
        if (warned.compareAndSet(false, true)) {
            log.restoringPulsarMessageType(raw.getTypeName());
        }
        if (isBatchType(raw)) {
            return new RestoredPulsarBatchMessage<>(in);
        }
        return new RestoredPulsarMessage<>(in);
    }

    private static boolean isMessageType(Type raw) {
        return raw.equals(PulsarMessage.class) || raw.equals(PulsarIdMessage.class)
                || raw.equals(PulsarIncomingMessage.class);
    }

    private static boolean isBatchType(Type raw) {
        return raw.equals(PulsarBatchMessage.class) || raw.equals(PulsarIncomingBatchMessage.class);
    }

    /**
     * Forwards metadata injected into a restored message to the intercepted message, if it accepts injections, and to
     * the original Pulsar message, whose metadata the acknowledgement handlers read, if the intercepted message
     * unwraps to it.
     *
     * @param intercepted the intercepted message
     * @param metadata the injected metadata
     * @param originalType the type of the original Pulsar message
     */
    static void forwardInjection(Message<?> intercepted, Object metadata, Class<?> originalType) {
        if (intercepted instanceof MetadataInjectableMessage<?> injectable) {
            injectable.injectMetadata(metadata);
        }
        Object original;
        try {
            original = intercepted.unwrap(originalType);
        } catch (IllegalArgumentException e) {
            // The original message is not reachable: the restored and the intercepted messages keep the metadata.
            return;
        }
        if (original != intercepted && original instanceof MetadataInjectableMessage<?> injectable) {
            // Injecting the same metadata type again replaces it: no harm if the intercepted message forwarded it.
            injectable.injectMetadata(metadata);
        }
    }

    /**
     * A Pulsar message restored from an intercepted message: payload and metadata come from that message, the Pulsar
     * message from its {@link PulsarIncomingMessageMetadata}, and acknowledgement is delegated to it.
     */
    static class RestoredPulsarMessage<T> extends PulsarIncomingMessage<T> {

        private final Message<?> intercepted;

        @SuppressWarnings("unchecked")
        RestoredPulsarMessage(Message<?> intercepted) {
            super((org.apache.pulsar.client.api.Message<T>) intercepted.getMetadata(PulsarIncomingMessageMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No Pulsar message metadata"))
                    .getMessage(), intercepted.getMetadata());
            this.intercepted = intercepted;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T getPayload() {
            return (T) intercepted.getPayload();
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            return intercepted.ack(metadata);
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return intercepted.nack(reason, metadata);
        }

        @Override
        public void injectMetadata(Object metadataObject) {
            super.injectMetadata(metadataObject);
            forwardInjection(intercepted, metadataObject, PulsarIncomingMessage.class);
        }

        @Override
        public <C> C unwrap(Class<C> unwrapType) {
            if (unwrapType != null && unwrapType.isInstance(this)) {
                return unwrapType.cast(this);
            }
            return intercepted.unwrap(unwrapType);
        }
    }

    /**
     * A Pulsar batch message restored from an intercepted message: payload and metadata come from that message, the
     * Pulsar messages and the incoming messages from its {@link PulsarIncomingBatchMessageMetadata}, and
     * acknowledgement is delegated to it.
     */
    static class RestoredPulsarBatchMessage<T> extends PulsarIncomingBatchMessage<T> {

        private final Message<?> intercepted;

        RestoredPulsarBatchMessage(Message<?> intercepted) {
            this(intercepted, intercepted.getMetadata(PulsarIncomingBatchMessageMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No Pulsar batch message metadata")));
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private RestoredPulsarBatchMessage(Message<?> intercepted, PulsarIncomingBatchMessageMetadata batchMetadata) {
            super(batchMetadata.getMessages(), (List<PulsarMessage<T>>) (List) batchMetadata.getIncomingMessages(),
                    (List<T>) intercepted.getPayload(), intercepted.getMetadata());
            this.intercepted = intercepted;
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            // The intercepted batch message injects its transaction metadata, if any, into the messages of the batch
            // when acknowledged. Inject the one of this message too, in case it could not be forwarded to it.
            getMetadata(PulsarTransactionMetadata.class).ifPresent(txn -> {
                for (PulsarMessage<T> message : getMessages()) {
                    if (message instanceof MetadataInjectableMessage<?> injectable) {
                        injectable.injectMetadata(txn);
                    }
                }
            });
            return intercepted.ack(metadata);
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return intercepted.nack(reason, metadata);
        }

        @Override
        public void injectMetadata(Object metadataObject) {
            super.injectMetadata(metadataObject);
            forwardInjection(intercepted, metadataObject, PulsarIncomingBatchMessage.class);
        }

        @Override
        public <C> C unwrap(Class<C> unwrapType) {
            if (unwrapType != null && unwrapType.isInstance(this)) {
                return unwrapType.cast(this);
            }
            return intercepted.unwrap(unwrapType);
        }
    }
}
