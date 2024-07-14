package io.smallrye.reactive.messaging.kafka.tracing;

import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_CONSUMER_ID;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_KAFKA_CONSUMER_GROUP;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_KAFKA_MESSAGE_OFFSET;
import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_KAFKA_PARTITION;

import java.util.Collections;
import java.util.List;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

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
        public String getSystem(final KafkaTrace kafkaTrace) {
            return "kafka";
        }

        @Override
        public String getDestination(final KafkaTrace kafkaTrace) {
            return kafkaTrace.getTopic();
        }

        @Override
        public boolean isTemporaryDestination(final KafkaTrace kafkaTrace) {
            return false;
        }

        @Override
        public String getConversationId(final KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public String getMessageId(final KafkaTrace kafkaTrace, final Void unused) {
            return null;
        }

        @Override
        public List<String> getMessageHeader(KafkaTrace kafkaTrace, String name) {
            return Collections.emptyList();
        }

        @Override
        public String getDestinationTemplate(KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public boolean isAnonymousDestination(KafkaTrace kafkaTrace) {
            return false;
        }

        @Override
        public Long getMessageBodySize(KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public Long getMessageEnvelopeSize(KafkaTrace kafkaTrace) {
            return null;
        }

        @Override
        public String getClientId(KafkaTrace kafkaTrace) {
            if (kafkaTrace.getClientId() == null) {
                return null;
            }
            return kafkaTrace.getClientId();
        }

        @Override
        public Long getBatchMessageCount(KafkaTrace kafkaTrace, Void unused) {
            return null;
        }
    }
}
