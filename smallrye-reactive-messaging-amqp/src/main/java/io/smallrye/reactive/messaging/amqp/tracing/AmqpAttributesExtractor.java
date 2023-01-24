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
        public String system(final AmqpMessage<?> amqpMessage) {
            return "AMQP 1.0";
        }

        // Required if the message destination is either a queue or topic
        @Override
        public String destinationKind(final AmqpMessage<?> amqpMessage) {
            return "queue";
        }

        // Required
        @Override
        public String destination(final AmqpMessage<?> amqpMessage) {
            return amqpMessage.getAddress();
        }

        @Override
        public boolean temporaryDestination(final AmqpMessage<?> amqpMessage) {
            return false;
        }

        // Recommended
        @Override
        public String protocol(final AmqpMessage<?> amqpMessage) {
            return "AMQP";
        }

        // Recommended
        @Override
        public String protocolVersion(final AmqpMessage<?> amqpMessage) {
            return "1.0";
        }

        // Recommended
        @Override
        public String url(final AmqpMessage<?> amqpMessage) {
            // TODO - radcortez - Need to get it from the configuration
            return null;
        }

        // Recommended
        @Override
        public String conversationId(final AmqpMessage<?> amqpMessage) {
            Object correlationId = amqpMessage.getCorrelationId();
            return correlationId instanceof String ? (String) correlationId : null;
        }

        // Recommended
        @Override
        public Long messagePayloadSize(final AmqpMessage<?> amqpMessage) {
            return null;
        }

        // Recommended
        @Override
        public Long messagePayloadCompressedSize(final AmqpMessage<?> amqpMessage) {
            return null;
        }

        // Recommended
        @Override
        public String messageId(final AmqpMessage<?> amqpMessage, final Void unused) {
            Object messageId = amqpMessage.getMessageId();
            return messageId instanceof String ? (String) messageId : null;
        }
    }
}
