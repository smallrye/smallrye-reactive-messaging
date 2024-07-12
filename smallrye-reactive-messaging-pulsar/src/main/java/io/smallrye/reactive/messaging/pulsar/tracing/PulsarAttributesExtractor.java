package io.smallrye.reactive.messaging.pulsar.tracing;

import static io.opentelemetry.semconv.SemanticAttributes.MESSAGING_CONSUMER_ID;

import java.util.Collections;
import java.util.List;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

public class PulsarAttributesExtractor implements AttributesExtractor<PulsarTrace, Void> {
    private final MessagingAttributesGetter<PulsarTrace, Void> messagingAttributesGetter;

    public PulsarAttributesExtractor() {
        this.messagingAttributesGetter = new PulsarMessagingAttributesGetter();
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final PulsarTrace pulsarTrace) {
        String consumerName = pulsarTrace.getConsumerName();
        attributes.put(MESSAGING_CONSUMER_ID, consumerName);
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final PulsarTrace pulsarTrace,
            final Void unused,
            final Throwable error) {

    }

    public MessagingAttributesGetter<PulsarTrace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    private static final class PulsarMessagingAttributesGetter implements MessagingAttributesGetter<PulsarTrace, Void> {
        @Override
        public String getSystem(PulsarTrace pulsarTrace) {
            return "pulsar";
        }

        @Override
        public String getDestination(PulsarTrace pulsarTrace) {
            return pulsarTrace.getTopic();
        }

        @Override
        public boolean isTemporaryDestination(PulsarTrace pulsarTrace) {
            return false;
        }

        @Override
        public String getConversationId(PulsarTrace pulsarTrace) {
            return null;
        }

        @Override
        public String getMessageId(PulsarTrace pulsarTrace, Void unused) {
            return pulsarTrace.getMessageId();
        }

        @Override
        public List<String> getMessageHeader(PulsarTrace pulsarTrace, String name) {
            return Collections.emptyList();
        }

        @Override
        public String getDestinationTemplate(PulsarTrace pulsarTrace) {
            return null;
        }

        @Override
        public boolean isAnonymousDestination(PulsarTrace pulsarTrace) {
            return false;
        }

        @Override
        public Long getMessageBodySize(PulsarTrace pulsarTrace) {
            return pulsarTrace.getUncompressedPayloadSize();
        }

        @Override
        public Long getMessageEnvelopeSize(PulsarTrace pulsarTrace) {
            return null;
        }

        @Override
        public String getClientId(PulsarTrace pulsarTrace) {
            return null;
        }

        @Override
        public Long getBatchMessageCount(PulsarTrace pulsarTrace, Void unused) {
            return null;
        }
    }
}
