package io.smallrye.reactive.messaging.kafka.tracing;

import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_CONSUMER_ID;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_KAFKA_CLIENT_ID;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_KAFKA_CONSUMER_GROUP;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_KAFKA_MESSAGE_OFFSET;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_KAFKA_PARTITION;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;

public class KafkaAttributesExtractor implements AttributesExtractor<KafkaTrace, Void> {
    private final MessagingAttributesGetter<KafkaTrace, Void> messagingAttributesGetter;

    public KafkaAttributesExtractor() {
        this.messagingAttributesGetter = new KafkaMessagingAttributesGetter();
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final KafkaTrace kafkaTrace) {
        if (kafkaTrace.getPartition() != -1) {
            attributes.put(MESSAGING_KAFKA_PARTITION, kafkaTrace.getPartition());
        }
        if (kafkaTrace.getOffset() != -1) {
            attributes.put(MESSAGING_KAFKA_MESSAGE_OFFSET, kafkaTrace.getOffset());
        }

        String groupId = kafkaTrace.getGroupId();
        String clientId = kafkaTrace.getClientId();
        if (groupId != null && clientId != null) {
            String consumerId = groupId;
            if (!clientId.isEmpty()) {
                consumerId += " - " + clientId;
            }
            attributes.put(MESSAGING_CONSUMER_ID, consumerId);
        }
        if (groupId != null) {
            attributes.put(MESSAGING_KAFKA_CONSUMER_GROUP, groupId);
        }
        if (clientId != null) {
            attributes.put(MESSAGING_KAFKA_CLIENT_ID, clientId);
        }
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final KafkaTrace kafkaTrace,
            final Void unused,
            final Throwable error) {

    }

    public MessagingAttributesGetter<KafkaTrace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    private static final class KafkaMessagingAttributesGetter implements MessagingAttributesGetter<KafkaTrace, Void> {
        @Override
        public String system(final KafkaTrace kafkaTrace) {
            return "kafka";
        }

        @Override
        public String destinationKind(final KafkaTrace kafkaTrace) {
            return "topic";
        }

        @Override
        public String destination(final KafkaTrace kafkaTrace) {
            return kafkaTrace.getTopic();
        }

        @Override
        public boolean temporaryDestination(final KafkaTrace kafkaTrace) {
            return false;
        }

        @Override
        public String protocol(final KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public String protocolVersion(final KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public String url(final KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public String conversationId(final KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public Long messagePayloadSize(final KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public Long messagePayloadCompressedSize(final KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public String messageId(final KafkaTrace kafkaTrace, final Void unused) {
            return null;
        }
    }
}
