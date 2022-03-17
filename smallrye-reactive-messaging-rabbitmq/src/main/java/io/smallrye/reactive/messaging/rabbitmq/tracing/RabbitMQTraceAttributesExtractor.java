package io.smallrye.reactive.messaging.rabbitmq.tracing;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;

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
        // TODO - radcortez - What to add here?
        // attributes.put(MESSAGING_CONSUMER_ID, null);
        // attributes.put(MESSAGING_RABBITMQ_ROUTING_KEY, null);
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final RabbitMQTrace rabbitMQTrace, final Void unused, final Throwable error) {
    }

    private final static class RabbitMQMessagingAttributesGetter implements MessagingAttributesGetter<RabbitMQTrace, Void> {
        @Override
        public String system(final RabbitMQTrace rabbitMQTrace) {
            return RabbitMQConnector.CONNECTOR_NAME;
        }

        @Override
        public String destinationKind(final RabbitMQTrace rabbitMQTrace) {
            return "queue";
        }

        @Override
        public String destination(final RabbitMQTrace rabbitMQTrace) {
            return rabbitMQTrace.getDestination();
        }

        @Override
        public boolean temporaryDestination(final RabbitMQTrace rabbitMQTrace) {
            return false;
        }

        @Override
        public String protocol(final RabbitMQTrace rabbitMQTrace) {
            return "AMQP";
        }

        @Override
        public String protocolVersion(final RabbitMQTrace rabbitMQTrace) {
            return "1.0";
        }

        @Override
        public String url(final RabbitMQTrace rabbitMQTrace) {
            return null;
        }

        @Override
        public String conversationId(final RabbitMQTrace rabbitMQTrace) {
            return null;
        }

        @Override
        public Long messagePayloadSize(final RabbitMQTrace rabbitMQTrace) {
            return null;
        }

        @Override
        public Long messagePayloadCompressedSize(final RabbitMQTrace rabbitMQTrace) {
            return null;
        }

        @Override
        public String messageId(final RabbitMQTrace rabbitMQTrace, final Void unused) {
            return null;
        }
    }
}
