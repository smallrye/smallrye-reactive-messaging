package io.smallrye.reactive.messaging.rabbitmq.tracing;

import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_RABBITMQ_ROUTING_KEY;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;

public class RabbitMQTraceAttributesExtractor implements AttributesExtractor<RabbitMQTrace, Void> {
    private final MessagingAttributesGetter<RabbitMQTrace, Void> messagingAttributesGetter;

    public RabbitMQTraceAttributesExtractor() {
        this.messagingAttributesGetter = new RabbitMQMessagingAttributesGetter();
    }

    public MessagingAttributesGetter<RabbitMQTrace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    @Override
    public void onStart(
            final AttributesBuilder attributes,
            final Context parentContext, final RabbitMQTrace rabbitMQTrace) {
        attributes.put(MESSAGING_RABBITMQ_ROUTING_KEY, rabbitMQTrace.getRoutingKey());
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final RabbitMQTrace rabbitMQTrace, final Void unused, final Throwable error) {
    }

    private final static class RabbitMQMessagingAttributesGetter implements MessagingAttributesGetter<RabbitMQTrace, Void> {
        @Override
        public String getSystem(final RabbitMQTrace rabbitMQTrace) {
            return "rabbitmq";
        }

        @Override
        public String getDestination(final RabbitMQTrace rabbitMQTrace) {
            return rabbitMQTrace.getDestination();
        }

        @Override
        public boolean isTemporaryDestination(RabbitMQTrace rabbitMQTrace) {
            return false;
        }

        @Override
        public String getConversationId(final RabbitMQTrace rabbitMQTrace) {
            return null;
        }

        @Override
        public Long getMessagePayloadSize(final RabbitMQTrace rabbitMQTrace) {
            return null;
        }

        @Override
        public Long getMessagePayloadCompressedSize(final RabbitMQTrace rabbitMQTrace) {
            return null;
        }

        @Override
        public String getMessageId(final RabbitMQTrace rabbitMQTrace, final Void unused) {
            return null;
        }
    }
}
