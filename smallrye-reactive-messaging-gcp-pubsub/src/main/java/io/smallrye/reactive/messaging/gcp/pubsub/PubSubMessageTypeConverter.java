package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubLogging.log;

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
 * Restores the Pub/Sub message type ({@link PubSubMessage}) for methods consuming it, when the message lost it on the
 * way: features that intercept messages (interceptors, observation, graceful shutdown tracking) rebuild messages,
 * keeping the payload, the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and the Pub/Sub message from the
 * {@link IncomingPubSubMetadata} of that metadata. It delegates acknowledgement and negative acknowledgement to the
 * intercepted message, so the interception keeps working.
 */
@ApplicationScoped
public class PubSubMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.getRawTypeIfParameterized(target).equals(PubSubMessage.class)
                && !(in instanceof PubSubMessage)
                && in.getMetadata(IncomingPubSubMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        if (warned.compareAndSet(false, true)) {
            log.restoringPubSubMessageType(TypeUtils.getRawTypeIfParameterized(target).getTypeName());
        }
        return new RestoredPubSubMessage(in);
    }

    /**
     * A Pub/Sub message restored from an intercepted message: payload and metadata come from that message, the Pub/Sub
     * message from its Pub/Sub metadata, and acknowledgement is delegated to it.
     */
    static class RestoredPubSubMessage extends PubSubMessage {

        private final Message<?> delegate;

        RestoredPubSubMessage(Message<?> delegate) {
            super(delegate.getMetadata(IncomingPubSubMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No Pub/Sub message metadata"))
                    .getMessage());
            this.delegate = delegate;
        }

        @Override
        public String getPayload() {
            // The payload of a Pub/Sub message is its data as text: keep it if an interception changed its type.
            return delegate.getPayload() instanceof String text ? text : super.getPayload();
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
            return this::ack;
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return delegate.nack(reason, metadata);
        }

        @Override
        public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
            return this::nack;
        }
    }
}
