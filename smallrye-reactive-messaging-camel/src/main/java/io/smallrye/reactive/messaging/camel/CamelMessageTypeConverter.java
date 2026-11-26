package io.smallrye.reactive.messaging.camel;

import static io.smallrye.reactive.messaging.camel.i18n.CamelLogging.log;

import java.lang.reflect.Type;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

/**
 * Restores the Camel message type ({@link CamelMessage}) for methods consuming it, when the message lost it on the
 * way: features that intercept messages (interceptors, observation, graceful shutdown tracking) rebuild messages,
 * keeping the payload, the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and the exchange from the
 * {@link IncomingExchangeMetadata} of that metadata. It delegates acknowledgement and negative acknowledgement to the
 * intercepted message, so the interception keeps working, and the intercepted message nacks the original one, which
 * applies the failure strategy of the channel.
 */
@ApplicationScoped
public class CamelMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.getRawTypeIfParameterized(target).equals(CamelMessage.class)
                && !(in instanceof CamelMessage)
                && in.getMetadata(IncomingExchangeMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        if (warned.compareAndSet(false, true)) {
            log.restoringCamelMessageType(TypeUtils.getRawTypeIfParameterized(target).getTypeName());
        }
        return new RestoredCamelMessage<>(in);
    }

    /**
     * A Camel message restored from an intercepted message: payload and metadata come from that message, the exchange
     * from its exchange metadata, and acknowledgement is delegated to it.
     */
    static class RestoredCamelMessage<T> extends CamelMessage<T> {

        private final Message<T> delegate;

        @SuppressWarnings("unchecked")
        RestoredCamelMessage(Message<?> delegate) {
            super(delegate.getMetadata(IncomingExchangeMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No Camel exchange metadata"))
                    .getExchange(), null);
            this.delegate = (Message<T>) delegate;
        }

        @Override
        public T getPayload() {
            return delegate.getPayload();
        }

        @Override
        public Metadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            return delegate.ack(metadata);
        }

        @Override
        public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
            return delegate::ack;
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return delegate.nack(reason, metadata);
        }

        @Override
        public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
            return delegate::nack;
        }
    }
}
