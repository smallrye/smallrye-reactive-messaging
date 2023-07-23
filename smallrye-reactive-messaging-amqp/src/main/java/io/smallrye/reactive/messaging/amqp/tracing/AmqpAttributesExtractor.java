package io.smallrye.reactive.messaging.amqp.tracing;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;

public class AmqpAttributesExtractor implements AttributesExtractor<AmqpMessage<?>, Void> {
    private final MessagingAttributesGetter<AmqpMessage<?>, Void> messagingAttributesGetter;

    public AmqpAttributesExtractor() {
        this.messagingAttributesGetter = new AmqpMessagingAttributesGetter();
    }

    public MessagingAttributesGetter<AmqpMessage<?>, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    @Override
    public void onStart(
            final AttributesBuilder attributes,
            final Context parentContext,
            final AmqpMessage<?> amqpMessage) {

    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final AmqpMessage<?> amqpMessage,
            final Void unused,
            final Throwable error) {
    }

    private static class AmqpMessagingAttributesGetter implements MessagingAttributesGetter<AmqpMessage<?>, Void> {
        // Required
        @Override
        public String getSystem(final AmqpMessage<?> amqpMessage) {
            return "AMQP 1.0";
        }

        // Required
        @Override
        public String getDestination(final AmqpMessage<?> amqpMessage) {
            return amqpMessage.getAddress();
        }

        @Override
        public boolean isTemporaryDestination(final AmqpMessage<?> amqpMessage) {
            return false;
        }

        // Recommended
        @Override
        public String getConversationId(final AmqpMessage<?> amqpMessage) {
            Object correlationId = amqpMessage.getCorrelationId();
            return correlationId instanceof String ? (String) correlationId : null;
        }

        // Recommended
        @Override
        public Long getMessagePayloadSize(final AmqpMessage<?> amqpMessage) {
            return null;
        }

        // Recommended
        @Override
        public Long getMessagePayloadCompressedSize(final AmqpMessage<?> amqpMessage) {
            return null;
        }

        // Recommended
        @Override
        public String getMessageId(final AmqpMessage<?> amqpMessage, final Void unused) {
            Object messageId = amqpMessage.getMessageId();
            return messageId instanceof String ? (String) messageId : null;
        }
    }
}
