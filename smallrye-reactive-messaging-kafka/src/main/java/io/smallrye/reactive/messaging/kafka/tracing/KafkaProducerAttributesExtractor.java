package io.smallrye.reactive.messaging.kafka.tracing;

import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_PARTITION_ID;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_KAFKA_OFFSET;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.producer.RecordMetadata;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

public class KafkaProducerAttributesExtractor implements AttributesExtractor<KafkaTrace, RecordMetadata> {

    private final MessagingAttributesGetter<KafkaTrace, RecordMetadata> messagingAttributesGetter;

    public KafkaProducerAttributesExtractor() {
        this.messagingAttributesGetter = new KafkaProducerMessagingAttributesGetter();
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final KafkaTrace kafkaTrace) {
        if (kafkaTrace.getPartition() != -1) {
            attributes.put(MESSAGING_DESTINATION_PARTITION_ID, Integer.toString(kafkaTrace.getPartition()));
        }
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final KafkaTrace kafkaTrace,
            final RecordMetadata response,
            final Throwable error) {
        if (response != null) {
            attributes.put(MESSAGING_DESTINATION_PARTITION_ID, Integer.toString(response.partition()));
            if (response.hasOffset()) {
                attributes.put(MESSAGING_KAFKA_OFFSET, response.offset());
            }
        }
    }

    public MessagingAttributesGetter<KafkaTrace, RecordMetadata> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    private static final class KafkaProducerMessagingAttributesGetter
            implements MessagingAttributesGetter<KafkaTrace, RecordMetadata> {
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
        public String getMessageId(final KafkaTrace kafkaTrace, final RecordMetadata response) {
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
            return kafkaTrace.getClientId();
        }

        @Override
        public Long getBatchMessageCount(KafkaTrace kafkaTrace, RecordMetadata response) {
            return null;
        }
    }
}
