package io.smallrye.reactive.messaging.gcp.pubsub.tracing;

import java.util.Collections;
import java.util.List;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

public class PubSubAttributesExtractor implements AttributesExtractor<PubSubTrace, Void> {
    private final MessagingAttributesGetter<PubSubTrace, Void> messagingAttributesGetter;

    public PubSubAttributesExtractor() {
        this.messagingAttributesGetter = new PubSubMessagingAttributesGetter();
    }

    public MessagingAttributesGetter<PubSubTrace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final PubSubTrace pubSubTrace) {
    }

    @Override
    public void onEnd(final AttributesBuilder attributes, final Context context, final PubSubTrace pubSubTrace,
            final Void unused, final Throwable error) {
    }

    private static class PubSubMessagingAttributesGetter implements MessagingAttributesGetter<PubSubTrace, Void> {
        @Override
        public String getSystem(final PubSubTrace pubSubTrace) {
            return "gcp_pubsub";
        }

        @Override
        public String getDestination(final PubSubTrace pubSubTrace) {
            return pubSubTrace.getTopic();
        }

        @Override
        public boolean isTemporaryDestination(final PubSubTrace pubSubTrace) {
            return false;
        }

        @Override
        public String getConversationId(final PubSubTrace pubSubTrace) {
            return null;
        }

        @Override
        public String getMessageId(final PubSubTrace pubSubTrace, final Void unused) {
            return null;
        }

        @Override
        public List<String> getMessageHeader(final PubSubTrace pubSubTrace, final String name) {
            return Collections.emptyList();
        }

        @Override
        public String getDestinationTemplate(final PubSubTrace pubSubTrace) {
            return null;
        }

        @Override
        public boolean isAnonymousDestination(final PubSubTrace pubSubTrace) {
            return false;
        }

        @Override
        public Long getMessageBodySize(final PubSubTrace pubSubTrace) {
            return null;
        }

        @Override
        public Long getMessageEnvelopeSize(final PubSubTrace pubSubTrace) {
            return null;
        }

        @Override
        public String getClientId(final PubSubTrace pubSubTrace) {
            return null;
        }

        @Override
        public Long getBatchMessageCount(final PubSubTrace pubSubTrace, final Void unused) {
            return null;
        }
    }
}
