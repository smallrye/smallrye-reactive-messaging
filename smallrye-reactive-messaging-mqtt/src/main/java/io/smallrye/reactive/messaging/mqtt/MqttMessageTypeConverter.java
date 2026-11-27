package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.lang.reflect.Type;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

/**
 * Restores the MQTT message type ({@link ReceivingMqttMessage}, {@link MqttMessage}) for methods consuming it, when the
 * message lost it on the way: features that intercept messages (interceptors, observation, graceful shutdown tracking)
 * rebuild messages, keeping the payload, the metadata and the acknowledgement, but not the connector's message type.
 * <p>
 * The restored message takes its payload and metadata from the intercepted message, and the MQTT message from the
 * {@link ReceivingMqttMessageMetadata} of that metadata. It delegates acknowledgement and negative acknowledgement to
 * the intercepted message, so the interception keeps working.
 */
@ApplicationScoped
public class MqttMessageTypeConverter implements MessageConverter {

    private final AtomicBoolean warned = new AtomicBoolean();

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        Type raw = TypeUtils.getRawTypeIfParameterized(target);
        return (raw.equals(ReceivingMqttMessage.class) || raw.equals(MqttMessage.class))
                && !(in instanceof MqttMessage)
                && in.getMetadata(ReceivingMqttMessageMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        if (warned.compareAndSet(false, true)) {
            log.restoringMqttMessageType(TypeUtils.getRawTypeIfParameterized(target).getTypeName());
        }
        return new RestoredMqttMessage(in);
    }

    /**
     * An MQTT message restored from an intercepted message: payload and metadata come from that message, the MQTT
     * message from its MQTT metadata, and acknowledgement is delegated to it.
     */
    static class RestoredMqttMessage extends ReceivingMqttMessage {

        private final Message<?> delegate;

        RestoredMqttMessage(Message<?> delegate) {
            super(delegate.getMetadata(ReceivingMqttMessageMetadata.class)
                    .orElseThrow(() -> new IllegalStateException("No MQTT message metadata"))
                    .getMessage(), null);
            this.delegate = delegate;
        }

        @Override
        public byte[] getPayload() {
            // The payload of an MQTT message is its bytes: keep them if an interception changed the payload type.
            return delegate.getPayload() instanceof byte[] bytes ? bytes : super.getPayload();
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
    }
}
